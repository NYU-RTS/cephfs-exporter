package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"path/filepath"
	"strconv"

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
	SkipIfSmaller uint64                   `json:"skip_if_smaller,omitempty"`
	Entries       map[string]*ReportConfig `json:"entries,omitempty"`
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
)

type Collector struct {
	prometheus.Collector
	filesystem   *cephfs.MountInfo
	reportConfig *ReportConfig
}

func (c Collector) Describe(ch chan<- *prometheus.Desc) {
	prometheus.DescribeByCollect(c, ch)
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
