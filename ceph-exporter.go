package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"path/filepath"
	"regexp"
	"strconv"
	"time"

	"github.com/ceph/go-ceph/cephfs"
	rados "github.com/ceph/go-ceph/rados"
	"github.com/ianschenck/envflag"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

const (
	defaultCephConfigPath = "/etc/ceph/ceph.conf"
	defaultCephUser       = "admin"
)

type ReportConfig struct {
	Report        bool                     `json:"report"`
	Snapshots     []ReportSnapshotConfig   `json:"snapshots,omitempty"`
	SkipIfSmaller uint64                   `json:"skip_if_smaller,omitempty"`
	Entries       map[string]*ReportConfig `json:"entries,omitempty"`
}

type ReportSnapshotConfig struct {
	Type   string         `json:"type"`
	Regexp *regexp.Regexp `json:"regexp,omitempty"`
}

func parseReportConfig(config string) (*ReportConfig, error) {
	buffer := bytes.NewBufferString(config)
	decoder := json.NewDecoder(buffer)
	decoder.DisallowUnknownFields()
	var reportConfig ReportConfig
	if err := decoder.Decode(&reportConfig); err != nil {
		return nil, err
	}
	return &reportConfig, nil
}

func (conf *ReportConfig) GetEntry(entry string) *ReportConfig {
	entryConf, found := conf.Entries[entry]
	if found {
		return entryConf
	}
	return conf.Entries["*"] // Might be nil
}

var (
	rbytesDesc = prometheus.NewDesc(
		"cephfs_rbytes",
		"Total size of directory in bytes",
		[]string{"path"}, nil,
	)
	rentriesDesc = prometheus.NewDesc(
		"cephfs_rentries",
		"Total number of files and subdirectories",
		[]string{"path"}, nil,
	)
	rfilesDesc = prometheus.NewDesc(
		"cephfs_rfiles",
		"Total number of files",
		[]string{"path"}, nil,
	)
	snapsDesc = prometheus.NewDesc(
		"cephfs_snaps",
		"Number of snapshots",
		[]string{"path", "type"}, nil,
	)
	snapsOldestDesc = prometheus.NewDesc(
		"cephfs_snap_oldest",
		"Timestamp of oldest snapshot",
		[]string{"path", "type"}, nil,
	)
	snapsNewestDesc = prometheus.NewDesc(
		"cephfs_snap_newest",
		"Timestamp of newest snapshot",
		[]string{"path", "type"}, nil,
	)
)

type Collector struct {
	prometheus.Collector
	filesystem   *cephfs.MountInfo
	reportConfig *ReportConfig
}

func (c Collector) Describe(ch chan<- *prometheus.Desc) {
	ch <- rbytesDesc
	ch <- rentriesDesc
	ch <- rfilesDesc
	ch <- snapsDesc
}

func (c Collector) Collect(ch chan<- prometheus.Metric) {
	err := c.observePath("/", ch, c.reportConfig)
	if err != nil {
		log.Print(err)
	}
}

func getNumXattr(filesystem *cephfs.MountInfo, path string, attr string) (uint64, error) {
	value, err := filesystem.GetXattr(path, attr)
	if err != nil {
		return 0, err
	}
	num, err := strconv.ParseUint(string(value), 10, 64)
	if err != nil {
		return 0, fmt.Errorf("Invalid number")
	}
	return num, nil
}

func (c Collector) observePath(path string, ch chan<- prometheus.Metric, reportConfig *ReportConfig) error {
	// Read rbytes
	rbytes, err := getNumXattr(c.filesystem, path, "ceph.dir.rbytes")
	if err != nil {
		return fmt.Errorf("Getting rbytes: %w", err)
	}

	// If this directory is too small, stop
	if rbytes < reportConfig.SkipIfSmaller {
		return nil
	}

	if reportConfig.Report {
		// Read entries
		rentries, err := getNumXattr(c.filesystem, path, "ceph.dir.rentries")
		if err != nil {
			return fmt.Errorf("Getting rentries: %w", err)
		}

		// Read files
		rfiles, err := getNumXattr(c.filesystem, path, "ceph.dir.rfiles")
		if err != nil {
			return fmt.Errorf("Getting rfiles: %w", err)
		}

		// Emit metrics
		ch <- prometheus.MustNewConstMetric(
			rbytesDesc,
			prometheus.GaugeValue,
			float64(rbytes),
			path,
		)
		ch <- prometheus.MustNewConstMetric(
			rentriesDesc,
			prometheus.GaugeValue,
			float64(rentries),
			path,
		)
		ch <- prometheus.MustNewConstMetric(
			rfilesDesc,
			prometheus.GaugeValue,
			float64(rfiles),
			path,
		)
	}

	if len(reportConfig.Snapshots) > 0 {
		// Gather list of all snapshots
		var snapshots []string
		dir, err := c.filesystem.OpenDir(path + "/.snap")
		if err != nil {
			return fmt.Errorf("Opening snap directory: %w", err)
		}
		for {
			entry, err := dir.ReadDir()
			if err != nil {
				return fmt.Errorf("Reading snap directory: %w", err)
			}
			if entry == nil {
				break
			}
			if entry.Name() == "." || entry.Name() == ".." {
				continue
			}
			snapshots = append(snapshots, entry.Name())
		}

		for _, metricConfig := range reportConfig.Snapshots {
			count := 0
			var oldest, newest *time.Time
			for _, snapshot := range snapshots {
				var match []string
				if metricConfig.Regexp != nil {
					match = metricConfig.Regexp.FindStringSubmatch(snapshot)
				}
				if metricConfig.Regexp == nil || match != nil {
					// It matches, count it
					count += 1

					// If there is a 'date' sub-expression, measure that
					if match != nil {
						dateSubexp := metricConfig.Regexp.SubexpIndex("date")
						if dateSubexp != -1 {
							date, err := time.Parse("2006-01-02-15_04_05", match[dateSubexp])
							if err != nil {
								continue
							}
							if oldest == nil || date.Before(*oldest) {
								oldest = &date
							}
							if newest == nil || newest.Before(date) {
								newest = &date
							}
						}
					}
				}
			}
			ch <- prometheus.MustNewConstMetric(
				snapsDesc,
				prometheus.GaugeValue,
				float64(count),
				path,
				metricConfig.Type,
			)
			if oldest != nil {
				ch <- prometheus.MustNewConstMetric(
					snapsOldestDesc,
					prometheus.GaugeValue,
					float64(oldest.Unix()),
					path,
					metricConfig.Type,
				)
				ch <- prometheus.MustNewConstMetric(
					snapsNewestDesc,
					prometheus.GaugeValue,
					float64(newest.Unix()),
					path,
					metricConfig.Type,
				)
			}
		}
	}

	// Recurse
	if len(reportConfig.Entries) > 0 {
		dir, err := c.filesystem.OpenDir(path)
		if err != nil {
			return fmt.Errorf("Opening directory: %w", err)
		}
		for {
			entryDir, err := dir.ReadDir()
			if err != nil {
				return fmt.Errorf("Reading directory: %w", err)
			}
			if entryDir == nil {
				break
			}
			if entryDir.Name() == "." || entryDir.Name() == ".." {
				continue
			}
			if entryDir.DType() == cephfs.DTypeDir {
				subReportConfig := reportConfig.GetEntry(entryDir.Name())
				if subReportConfig == nil {
					continue
				}
				err := c.observePath(
					filepath.Join(path, entryDir.Name()),
					ch,
					subReportConfig,
				)
				if err != nil {
					return err
				}
			}
		}
	}

	return nil
}

func main() {
	var (
		metricsAddr  = envflag.String("TELEMETRY_ADDR", ":9128", "Host:Port for metrics endpoint")
		metricsPath  = envflag.String("TELEMETRY_PATH", "/metrics", "URL path for metrics endpoint")
		cephConfig   = envflag.String("CEPH_CONFIG", defaultCephConfigPath, "Path to Ceph config file")
		cephUser     = envflag.String("CEPH_USER", defaultCephUser, "Ceph user to connect to cluster")
		reportConfig = envflag.String("REPORT_CONFIG", "{\"report\": true}", "Report configuration")
	)

	envflag.Parse()
	conn, err := rados.NewConnWithUser(*cephUser)
	if err != nil {
		log.Fatalf("Failed to create rados connection: %v", err)
	}
	err = conn.ReadConfigFile(*cephConfig)
	if err != nil {
		log.Fatalf("Failed to read config file: %s", err)
	}

	err = conn.ReadDefaultConfigFile()
	if err != nil {
		log.Fatalf("Failed to read config file: %v", err)
	}

	err = conn.Connect()
	if err != nil {
		log.Fatalf("Failed to connect to the cluster: %v", err)
	}
	defer conn.Shutdown()
	log.Print("Successfully connected to Ceph cluster!")

	filesystem, err := cephfs.CreateFromRados(conn)
	if err != nil {
		log.Fatalf("Failed to create cephfs mountinfo: %v", err)
	}

	if err := filesystem.Init(); err != nil {
		log.Fatalf("Failed to init filesystem: %v", err)
	}

	if err := filesystem.SetMountPerms(cephfs.NewUserPerm(0, 0, []int{0})); err != nil {
		log.Fatalf("Failed to set mount permissions: %v", err)
	}

	if err := filesystem.Mount(); err != nil {
		log.Fatalf("Failed to mount filesystem: %v", err)
	}
	defer filesystem.Unmount()
	log.Print("Successfully mounted Ceph filesystem!")

	parsedReportConfig, err := parseReportConfig(*reportConfig)
	if err != nil {
		log.Fatalf("Invalid report config: %v", err)
	}

	prometheus.MustRegister(Collector{
		filesystem:   filesystem,
		reportConfig: parsedReportConfig,
	})
	http.Handle(*metricsPath, promhttp.Handler())

	log.Printf("Starting server on %s\n", *metricsAddr)
	log.Fatal(http.ListenAndServe(*metricsAddr, nil))
}
