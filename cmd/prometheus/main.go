// Copyright 2015 The Prometheus Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// The main package for the Prometheus server executable.
package main

import (
	"context"
	"fmt"
	"net"
	"net/http"
	_ "net/http/pprof" // Comment this line to disable pprof endpoint.
	"net/url"
	"os"
	"os/signal"
	"path/filepath"
	"regexp"
	"runtime"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	conntrack "github.com/mwitkow/go-conntrack"
	"github.com/oklog/run"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/model"
	"github.com/prometheus/common/promlog"
	"github.com/prometheus/common/version"
	kingpin "gopkg.in/alecthomas/kingpin.v2"
	"k8s.io/klog"

	promlogflag "github.com/prometheus/common/promlog/flag"
	"github.com/prometheus/prometheus/config"
	"github.com/prometheus/prometheus/discovery"
	sd_config "github.com/prometheus/prometheus/discovery/config"
	"github.com/prometheus/prometheus/notifier"
	"github.com/prometheus/prometheus/pkg/relabel"
	prom_runtime "github.com/prometheus/prometheus/pkg/runtime"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/rules"
	"github.com/prometheus/prometheus/scrape"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/storage/remote"
	"github.com/prometheus/prometheus/storage/tsdb"
	"github.com/prometheus/prometheus/util/strutil"
	"github.com/prometheus/prometheus/web"
)

var (
	configSuccess = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "prometheus_config_last_reload_successful",
		Help: "Whether the last configuration reload attempt was successful.",
	})
	configSuccessTime = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "prometheus_config_last_reload_success_timestamp_seconds",
		Help: "Timestamp of the last successful configuration reload.",
	})

	defaultRetentionString   = "15d"
	defaultRetentionDuration model.Duration
)

func init() {
	prometheus.MustRegister(version.NewCollector("prometheus"))

	var err error
	defaultRetentionDuration, err = model.ParseDuration(defaultRetentionString)
	if err != nil {
		panic(err)
	}
}

func main() {
	// Prothemus 进入调试模式, 需设置环境变量 DEBUG(重启 Prometheus 服务后生效)
	if os.Getenv("DEBUG") != "" {
		// Prometheus 使用 pprof对自身的性能数据进行采集和监控
		// 通过访问 http://localhost:9090/debug/pprof, 可以获取 Prometheus 具体的性能数据,
		// 如 block、goroutine、heap 等

		// SetBlockProfileRate 用于设置 goroutine 阻塞分析器的采样频率
		runtime.SetBlockProfileRate(20)
		// SetMutexProfileFraction 用于设置 goroutine 互斥锁竞争的采样频率
		runtime.SetMutexProfileFraction(20)
	}

	var (
		oldFlagRetentionDuration model.Duration
		newFlagRetentionDuration model.Duration
	)

	cfg := struct {
		configFile string

		localStoragePath    string
		notifier            notifier.Options
		notifierTimeout     model.Duration
		forGracePeriod      model.Duration
		outageTolerance     model.Duration
		resendDelay         model.Duration
		web                 web.Options
		tsdb                tsdb.Options
		lookbackDelta       model.Duration
		webTimeout          model.Duration
		queryTimeout        model.Duration
		queryConcurrency    int
		queryMaxSamples     int
		RemoteFlushDeadline model.Duration

		prometheusURL   string
		corsRegexString string

		promlogConfig promlog.Config
	}{
		notifier: notifier.Options{
			Registerer: prometheus.DefaultRegisterer,
		},
		web: web.Options{
			Registerer: prometheus.DefaultRegisterer,
			Gatherer:   prometheus.DefaultGatherer,
		},
		promlogConfig: promlog.Config{},
	}

	a := kingpin.New(filepath.Base(os.Args[0]), "The Prometheus monitoring server")

	a.Version(version.Print("prometheus"))

	a.HelpFlag.Short('h')

	a.Flag("config.file", "Prometheus configuration file path.").
		Default("prometheus.yml").StringVar(&cfg.configFile)

	a.Flag("web.listen-address", "Address to listen on for UI, API, and telemetry.").
		Default("0.0.0.0:9090").StringVar(&cfg.web.ListenAddress)

	a.Flag("web.read-timeout",
		"Maximum duration before timing out read of the request, and closing idle connections.").
		Default("5m").SetValue(&cfg.webTimeout)

	a.Flag("web.max-connections", "Maximum number of simultaneous connections.").
		Default("512").IntVar(&cfg.web.MaxConnections)

	a.Flag("web.external-url",
		"The URL under which Prometheus is externally reachable (for example, if Prometheus is served via a reverse proxy). Used for generating relative and absolute links back to Prometheus itself. If the URL has a path portion, it will be used to prefix all HTTP endpoints served by Prometheus. If omitted, relevant URL components will be derived automatically.").
		PlaceHolder("<URL>").StringVar(&cfg.prometheusURL)

	a.Flag("web.route-prefix",
		"Prefix for the internal routes of web endpoints. Defaults to path of --web.external-url.").
		PlaceHolder("<path>").StringVar(&cfg.web.RoutePrefix)

	a.Flag("web.user-assets", "Path to static asset directory, available at /user.").
		PlaceHolder("<path>").StringVar(&cfg.web.UserAssetsPath)

	a.Flag("web.enable-lifecycle", "Enable shutdown and reload via HTTP request.").
		Default("false").BoolVar(&cfg.web.EnableLifecycle)

	a.Flag("web.enable-admin-api", "Enable API endpoints for admin control actions.").
		Default("false").BoolVar(&cfg.web.EnableAdminAPI)

	a.Flag("web.console.templates", "Path to the console template directory, available at /consoles.").
		Default("consoles").StringVar(&cfg.web.ConsoleTemplatesPath)

	a.Flag("web.console.libraries", "Path to the console library directory.").
		Default("console_libraries").StringVar(&cfg.web.ConsoleLibrariesPath)

	a.Flag("web.page-title", "Document title of Prometheus instance.").
		Default("Prometheus Time Series Collection and Processing Server").StringVar(&cfg.web.PageTitle)

	a.Flag("web.cors.origin", `Regex for CORS origin. It is fully anchored. Example: 'https?://(domain1|domain2)\.com'`).
		Default(".*").StringVar(&cfg.corsRegexString)

	a.Flag("storage.tsdb.path", "Base path for metrics storage.").
		Default("data/").StringVar(&cfg.localStoragePath)

	a.Flag("storage.tsdb.min-block-duration", "Minimum duration of a data block before being persisted. For use in testing.").
		Hidden().Default("2h").SetValue(&cfg.tsdb.MinBlockDuration)

	a.Flag("storage.tsdb.max-block-duration",
		"Maximum duration compacted blocks may span. For use in testing. (Defaults to 10% of the retention period.)").
		Hidden().PlaceHolder("<duration>").SetValue(&cfg.tsdb.MaxBlockDuration)

	a.Flag("storage.tsdb.wal-segment-size",
		"Size at which to split the tsdb WAL segment files. Example: 100MB").
		Hidden().PlaceHolder("<bytes>").BytesVar(&cfg.tsdb.WALSegmentSize)

	a.Flag("storage.tsdb.retention", "[DEPRECATED] How long to retain samples in storage. This flag has been deprecated, use \"storage.tsdb.retention.time\" instead.").
		SetValue(&oldFlagRetentionDuration)

	a.Flag("storage.tsdb.retention.time", "How long to retain samples in storage. When this flag is set it overrides \"storage.tsdb.retention\". If neither this flag nor \"storage.tsdb.retention\" nor \"storage.tsdb.retention.size\" is set, the retention time defaults to "+defaultRetentionString+". Units Supported: y, w, d, h, m, s, ms.").
		SetValue(&newFlagRetentionDuration)

	a.Flag("storage.tsdb.retention.size", "[EXPERIMENTAL] Maximum number of bytes that can be stored for blocks. Units supported: KB, MB, GB, TB, PB. This flag is experimental and can be changed in future releases.").
		BytesVar(&cfg.tsdb.MaxBytes)

	a.Flag("storage.tsdb.no-lockfile", "Do not create lockfile in data directory.").
		Default("false").BoolVar(&cfg.tsdb.NoLockfile)

	a.Flag("storage.tsdb.allow-overlapping-blocks", "[EXPERIMENTAL] Allow overlapping blocks, which in turn enables vertical compaction and vertical query merge.").
		Default("false").BoolVar(&cfg.tsdb.AllowOverlappingBlocks)

	a.Flag("storage.tsdb.wal-compression", "Compress the tsdb WAL.").
		Default("false").BoolVar(&cfg.tsdb.WALCompression)

	a.Flag("storage.remote.flush-deadline", "How long to wait flushing sample on shutdown or config reload.").
		Default("1m").PlaceHolder("<duration>").SetValue(&cfg.RemoteFlushDeadline)

	a.Flag("storage.remote.read-sample-limit", "Maximum overall number of samples to return via the remote read interface, in a single query. 0 means no limit. This limit is ignored for streamed response types.").
		Default("5e7").IntVar(&cfg.web.RemoteReadSampleLimit)

	a.Flag("storage.remote.read-concurrent-limit", "Maximum number of concurrent remote read calls. 0 means no limit.").
		Default("10").IntVar(&cfg.web.RemoteReadConcurrencyLimit)

	a.Flag("storage.remote.read-max-bytes-in-frame", "Maximum number of bytes in a single frame for streaming remote read response types before marshalling. Note that client might have limit on frame size as well. 1MB as recommended by protobuf by default.").
		Default("1048576").IntVar(&cfg.web.RemoteReadBytesInFrame)

	a.Flag("rules.alert.for-outage-tolerance", "Max time to tolerate prometheus outage for restoring \"for\" state of alert.").
		Default("1h").SetValue(&cfg.outageTolerance)

	a.Flag("rules.alert.for-grace-period", "Minimum duration between alert and restored \"for\" state. This is maintained only for alerts with configured \"for\" time greater than grace period.").
		Default("10m").SetValue(&cfg.forGracePeriod)

	a.Flag("rules.alert.resend-delay", "Minimum amount of time to wait before resending an alert to Alertmanager.").
		Default("1m").SetValue(&cfg.resendDelay)

	a.Flag("alertmanager.notification-queue-capacity", "The capacity of the queue for pending Alertmanager notifications.").
		Default("10000").IntVar(&cfg.notifier.QueueCapacity)

	a.Flag("alertmanager.timeout", "Timeout for sending alerts to Alertmanager.").
		Default("10s").SetValue(&cfg.notifierTimeout)

	a.Flag("query.lookback-delta", "The maximum lookback duration for retrieving metrics during expression evaluations.").
		Default("5m").SetValue(&cfg.lookbackDelta)

	a.Flag("query.timeout", "Maximum time a query may take before being aborted.").
		Default("2m").SetValue(&cfg.queryTimeout)

	a.Flag("query.max-concurrency", "Maximum number of queries executed concurrently.").
		Default("20").IntVar(&cfg.queryConcurrency)

	a.Flag("query.max-samples", "Maximum number of samples a single query can load into memory. Note that queries will fail if they try to load more samples than this into memory, so this also limits the number of samples a query can return.").
		Default("50000000").IntVar(&cfg.queryMaxSamples)

	promlogflag.AddFlags(a, &cfg.promlogConfig)

	_, err := a.Parse(os.Args[1:])
	if err != nil {
		fmt.Fprintln(os.Stderr, errors.Wrapf(err, "Error parsing commandline arguments"))
		a.Usage(os.Args[1:])
		os.Exit(2)
	}

	logger := promlog.New(&cfg.promlogConfig)

	cfg.web.ExternalURL, err = computeExternalURL(cfg.prometheusURL, cfg.web.ListenAddress)
	if err != nil {
		fmt.Fprintln(os.Stderr, errors.Wrapf(err, "parse external URL %q", cfg.prometheusURL))
		os.Exit(2)
	}

	cfg.web.CORSOrigin, err = compileCORSRegexString(cfg.corsRegexString)
	if err != nil {
		fmt.Fprintln(os.Stderr, errors.Wrapf(err, "could not compile CORS regex string %q", cfg.corsRegexString))
		os.Exit(2)
	}

	cfg.web.ReadTimeout = time.Duration(cfg.webTimeout)
	// Default -web.route-prefix to path of -web.external-url.
	if cfg.web.RoutePrefix == "" {
		cfg.web.RoutePrefix = cfg.web.ExternalURL.Path
	}
	// RoutePrefix must always be at least '/'.
	cfg.web.RoutePrefix = "/" + strings.Trim(cfg.web.RoutePrefix, "/")

	{ // Time retention settings.
		if oldFlagRetentionDuration != 0 {
			level.Warn(logger).Log("deprecation_notice", "'storage.tsdb.retention' flag is deprecated use 'storage.tsdb.retention.time' instead.")
			cfg.tsdb.RetentionDuration = oldFlagRetentionDuration
		}

		// When the new flag is set it takes precedence.
		if newFlagRetentionDuration != 0 {
			cfg.tsdb.RetentionDuration = newFlagRetentionDuration
		}

		if cfg.tsdb.RetentionDuration == 0 && cfg.tsdb.MaxBytes == 0 {
			cfg.tsdb.RetentionDuration = defaultRetentionDuration
			level.Info(logger).Log("msg", "no time or size retention was set so using the default time retention", "duration", defaultRetentionDuration)
		}

		// Check for overflows. This limits our max retention to 100y.
		if cfg.tsdb.RetentionDuration < 0 {
			y, err := model.ParseDuration("100y")
			if err != nil {
				panic(err)
			}
			cfg.tsdb.RetentionDuration = y
			level.Warn(logger).Log("msg", "time retention value is too high. Limiting to: "+y.String())
		}
	}

	{ // Max block size  settings.
		if cfg.tsdb.MaxBlockDuration == 0 {
			maxBlockDuration, err := model.ParseDuration("31d")
			if err != nil {
				panic(err)
			}
			// When the time retention is set and not too big use to define the max block duration.
			if cfg.tsdb.RetentionDuration != 0 && cfg.tsdb.RetentionDuration/10 < maxBlockDuration {
				maxBlockDuration = cfg.tsdb.RetentionDuration / 10
			}

			cfg.tsdb.MaxBlockDuration = maxBlockDuration
		}
	}

	promql.LookbackDelta = time.Duration(cfg.lookbackDelta)
	promql.SetDefaultEvaluationInterval(time.Duration(config.DefaultGlobalConfig.EvaluationInterval))

	// Above level 6, the k8s client would log bearer tokens in clear-text.
	klog.ClampLevel(6)
	klog.SetLogger(log.With(logger, "component", "k8s_client_runtime"))

	level.Info(logger).Log("msg", "Starting Prometheus", "version", version.Info())
	level.Info(logger).Log("build_context", version.BuildContext())
	level.Info(logger).Log("host_details", prom_runtime.Uname())
	level.Info(logger).Log("fd_limits", prom_runtime.FdLimits())
	level.Info(logger).Log("vm_limits", prom_runtime.VmLimits())

	var (
		// Prometheus 对指标的存储采用的是时序数据库, localStorage 与 remoteStorage 在初始化时
		// 需要使用相同的时间基线(localStorage.StartTime)存储指标
		// 指标的存储周期默认为 15d, 可通过 Prometheus 命令行参数 --storage.tsdb.retention=15d 进行个性化设置.
		// localStorage 用于指标的本地存储; remoteStorage 用于指标的远程存储;
		// fanoutStorage 为 localStorage 与 remoteStorage 的读写代理器, 且 remoteStorage 可以有多个传入
		localStorage  = &tsdb.ReadyStorage{}
		remoteStorage = remote.NewStorage(log.With(logger, "component", "remote"), prometheus.DefaultRegisterer, localStorage.StartTime, cfg.localStoragePath, time.Duration(cfg.RemoteFlushDeadline))
		fanoutStorage = storage.NewFanout(logger, localStorage, remoteStorage)
	)

	var (
		// ctxWeb 用于跟踪控制对应的 goroutine
		// cancelWeb 用于结束对应的 goroutine
		ctxWeb, cancelWeb = context.WithCancel(context.Background())
		ctxRule           = context.Background()

		// notifier 组件用于告警通知, 在完成初始化后, notifier 组件内部会构建一个告警通知队列, 队列的大小
		// 由命令行参数 --alertmanager.notification-queue-capacity 确定, 默认值为 10000, 且告警信息通过
		// sendAlerts 方法发送给 AlertManager.
		// notifier 组件的构建通过调用 notifier.NewManager 方法完成, cfg.notifier 为 notifier 组件的配置
		// 信息, 调用 log.With 方法返回的 logger 用于记录系统日志
		notifierManager = notifier.NewManager(&cfg.notifier, log.With(logger, "component", "notifier"))

		// discoveryManagerScrape 组件用于发现指标采集服务, 对应 prometheus.yml 配置文件中 scrape_configs
		// 节点下的各种指标采集器(static_config、kubernetes_sd_config、openstack_sd_config、consul_sd_config 等)
		// discoveryManagerScrape 组件的构建通过调用 discovery.NewManager 方法完成, logger 参数用于记录系统日志
		ctxScrape, cancelScrape = context.WithCancel(context.Background())
		discoveryManagerScrape  = discovery.NewManager(ctxScrape, log.With(logger, "component", "discovery manager scrape"), discovery.Name("scrape"))

		// discoveryManagerScrape 组件用于发现告警通知服务, 与 prometheus.yml 配置文件中的 alerting 标签
		// 相关联
		// AlterManager 的 URL 地址在 Prometheus 的 1.x 版本中通过 Prometheus 启动命令行参数 -alertmanager.url
		// 指定, 到 Prometheus 的 2.x 版本后通过 prometheus.yml 指定, 格式如下:
		//   alerting:
		//     alertmanagers:
		//       static_configs:
		//       - targets:
		//         - "localhost:9093"
		// discoveryManagerNotify 组件的构建与 discoveryManagerScrape 组件的构建方式一样, 不同的是前者服务
		// 与 notify, 后者服务于 scrape
		ctxNotify, cancelNotify = context.WithCancel(context.Background())
		discoveryManagerNotify  = discovery.NewManager(ctxNotify, log.With(logger, "component", "discovery manager notify"), discovery.Name("notify"))

		// scrapeManager 组件用于管理对指标的采集, 并将所采集的指标存储到 fanoutStorage 中.
		//
		// scrapeManager 组件的采集周期在 prometheus.yml 配置文件中由 global 节点下的 scrape_interval 指定,
		// 且各个 job_name 可以在 scrape_configs 下进行个性化设置, 设置符合自身应用场景的 scrape_interval.
		//
		// prometheus.yml 配置文件中 global 下的 scrape_interval 作用域为全局, 所有 job_name 共用.
		// 在 scrape_configs 下 job_name 中所配置的 scrape_interval 作用域仅限所描述的 job_name.
		//
		// scrapeManager 组件的构建调用 scrape.NewManager 方法完成, logger 参数用于记录系统日志
		scrapeManager = scrape.NewManager(log.With(logger, "component", "scrape manager"), fanoutStorage)

		opts = promql.EngineOpts{
			Logger:             log.With(logger, "component", "query engine"),
			Reg:                prometheus.DefaultRegisterer,
			MaxConcurrent:      cfg.queryConcurrency,
			MaxSamples:         cfg.queryMaxSamples,
			Timeout:            time.Duration(cfg.queryTimeout),
			ActiveQueryTracker: promql.NewActiveQueryTracker(cfg.localStoragePath, cfg.queryConcurrency, log.With(logger, "component", "activeQueryTracker")),
		}

		// queryEngine 组件为规则查询计算引擎, 在初始化时会对查询超时时间(cfg.queryTimeout)和并发查询个数
		// (cfg.queryConcurrency) 进行设置
		//
		// queryEngine 组件的构建调用 promql.NewEngine 方法完成
		queryEngine = promql.NewEngine(opts)

		// ruleManager 组件整合了 fanoutStorage 组件、queryEngine 组件和 notifier 组件, 完成
		// 了从规则运算到告警发送的流程.
		//
		// 规则运算周期在 prometheus.yml 配置文件中由 global 节点下的 evaluation_interval 指定,
		// 各个 job_name 还可以在 scrape_configs 下进行个性化设置, 设置符合自身应用场景的规则
		// 运算周期(evaluation_interval).
		//
		// ruleManager 组件的构建调用 rules.NewManager 方法完成
		ruleManager = rules.NewManager(&rules.ManagerOptions{
			Appendable:      fanoutStorage, // 存储器
			TSDB:            localStorage,
			QueryFunc:       rules.EngineQueryFunc(queryEngine, fanoutStorage),         // 规则计算
			NotifyFunc:      sendAlerts(notifierManager, cfg.web.ExternalURL.String()), // 告警通知
			Context:         ctxRule,                                                   // 控制 ruleManager 组件中的 goroutine
			ExternalURL:     cfg.web.ExternalURL,                                       // Web 访问地址
			Registerer:      prometheus.DefaultRegisterer,                              // 系统指标注册器
			Logger:          log.With(logger, "component", "rule manager"),             // 系统日志记录
			OutageTolerance: time.Duration(cfg.outageTolerance),
			ForGracePeriod:  time.Duration(cfg.forGracePeriod),
			ResendDelay:     time.Duration(cfg.resendDelay),
		})
	)

	// Web 组件的初始化
	cfg.web.Context = ctxWeb
	cfg.web.TSDB = localStorage.Get
	cfg.web.Storage = fanoutStorage
	cfg.web.QueryEngine = queryEngine
	cfg.web.ScrapeManager = scrapeManager
	cfg.web.RuleManager = ruleManager
	cfg.web.Notifier = notifierManager
	cfg.web.TSDBCfg = cfg.tsdb

	// 用于记录 Prometheus 的版本信息, 查看地址为 http://localhost:9090/status
	cfg.web.Version = &web.PrometheusVersion{
		Version:   version.Version,
		Revision:  version.Revision,
		Branch:    version.Branch,
		BuildUser: version.BuildUser,
		BuildDate: version.BuildDate,
		GoVersion: version.GoVersion,
	}

	// 用于记录命令行配置信息, 查看地址为 http://localhost:9090/flags
	cfg.web.Flags = map[string]string{}

	// Exclude kingpin default flags to expose only Prometheus ones.
	boilerplateFlags := kingpin.New("", "").Version("")
	for _, f := range a.Model().Flags {
		if boilerplateFlags.GetFlag(f.Name) != nil {
			continue
		}

		cfg.web.Flags[f.Name] = f.Value.String()
	}

	// 构建 Web 组件, logger 参数用于记录系统日志, cfg.web 中记录了 Prometheus 的版本、配置及编译信息等
	// Depends on cfg.web.ScrapeManager so needs to be after cfg.web.ScrapeManager = scrapeManager.
	webHandler := web.New(log.With(logger, "component", "web"), &cfg.web)

	// Monitor outgoing connections on default transport with conntrack.
	http.DefaultTransport.(*http.Transport).DialContext = conntrack.NewDialContextFunc(
		conntrack.DialWithTracing(),
	)

	// 组件配置管理就是将相关组件的配置加载过程统一为 ApplyConfig 方法, 并储存到 reloaders 中进行统一调用
	reloaders := []func(cfg *config.Config) error{
		remoteStorage.ApplyConfig,
		webHandler.ApplyConfig,
		// The Scrape and notifier managers need to reload before the Discovery manager as
		// they need to read the most updated config when receiving the new targets list.
		scrapeManager.ApplyConfig,
		// discoveryManagerScrape 配置加载
		func(cfg *config.Config) error {
			c := make(map[string]sd_config.ServiceDiscoveryConfig)
			for _, v := range cfg.ScrapeConfigs {
				c[v.JobName] = v.ServiceDiscoveryConfig
			}
			return discoveryManagerScrape.ApplyConfig(c)
		},
		notifierManager.ApplyConfig,
		// discoveryManagerNotify 配置加载
		func(cfg *config.Config) error {
			c := make(map[string]sd_config.ServiceDiscoveryConfig)
			for k, v := range cfg.AlertingConfig.AlertmanagerConfigs.ToMap() {
				c[k] = v.ServiceDiscoveryConfig
			}
			return discoveryManagerNotify.ApplyConfig(c)
		},
		// 将 ruleManager 内置更新方法 Update 转换为 ApplyConfig 类型如下
		func(cfg *config.Config) error {
			// Get all rule files matching the configuration paths.
			var files []string
			// 遍历规则文件
			for _, pat := range cfg.RuleFiles {
				fs, err := filepath.Glob(pat)
				if err != nil {
					// The only error can be a bad pattern.
					return errors.Wrapf(err, "error retrieving rule files for %s", pat)
				}
				files = append(files, fs...)
			}
			// 规则管理器(ruleManager)内置的配置更新方法为 ruleManager.Update.
			// 该方法用于更新查询引擎所计算的记录规则、告警规则及计算周期.
			return ruleManager.Update(
				time.Duration(cfg.GlobalConfig.EvaluationInterval),
				files,
				cfg.GlobalConfig.ExternalLabels,
			)
		},
	}

	prometheus.MustRegister(configSuccess)
	prometheus.MustRegister(configSuccessTime)

	// Start all components while we wait for TSDB to open but only load
	// initial config and mark ourselves as ready after it completed.
	dbOpen := make(chan struct{})

	// sync.Once is used to make sure we can close the channel at different execution stages(SIGTERM or when the config is loaded).
	type closeOnce struct {
		C     chan struct{}
		once  sync.Once
		Close func()
	}
	// Wait until the server is ready to handle reloading.
	reloadReady := &closeOnce{
		C: make(chan struct{}),
	}
	reloadReady.Close = func() {
		reloadReady.once.Do(func() {
			close(reloadReady.C)
		})
	}

	// Prometheus 各服务组件的启动流程由 10 次 group 的 Add 方法调用链构成
	var g run.Group
	{
		// Termination handler.
		term := make(chan os.Signal, 1)
		signal.Notify(term, os.Interrupt, syscall.SIGTERM)
		cancel := make(chan struct{})
		// 用于控制 Prometheus 程序的退出. 当满足以下任一条件时将退出:
		// - Prometheus 接收到 SIGTERM 系统信号;
		// - Prometheus 程序设置了 --web.enable-lifecycle 参数来启动且接收到
		//   "curl -X POST localhost:9090/-/quit" 请求
		g.Add(
			func() error {
				// Don't forget to release the reloadReady channel so that waiting blocks can exit normally.
				select {
				case <-term:
					level.Warn(logger).Log("msg", "Received SIGTERM, exiting gracefully...")
					reloadReady.Close()
				case <-webHandler.Quit():
					level.Warn(logger).Log("msg", "Received termination request via web service, exiting gracefully...")
				case <-cancel:
					reloadReady.Close()
				}
				return nil
			},
			func(err error) {
				close(cancel)
			},
		)
	}
	{
		// Scrape discovery manager.
		// 用于启动 DiscoveryManagerScrape 服务
		g.Add(
			func() error {
				err := discoveryManagerScrape.Run()
				level.Info(logger).Log("msg", "Scrape discovery manager stopped")
				return err
			},
			func(err error) {
				level.Info(logger).Log("msg", "Stopping scrape discovery manager...")
				cancelScrape()
			},
		)
	}
	{
		// Notify discovery manager.
		// 用于启动 discoveryManagerNotify 服务
		g.Add(
			func() error {
				err := discoveryManagerNotify.Run()
				level.Info(logger).Log("msg", "Notify discovery manager stopped")
				return err
			},
			func(err error) {
				level.Info(logger).Log("msg", "Stopping notify discovery manager...")
				cancelNotify()
			},
		)
	}
	{
		// 根据在第 2 个 Add 中 discoveryManagerScrape 发现的 scrape 服务启动且采集指标数据
		// Scrape manager.
		g.Add(
			func() error {
				// When the scrape manager receives a new targets list
				// it needs to read a valid config for each job.
				// It depends on the config being in sync with the discovery manager so
				// we wait until the config is fully loaded.
				<-reloadReady.C

				err := scrapeManager.Run(discoveryManagerScrape.SyncCh())
				level.Info(logger).Log("msg", "Scrape manager stopped")
				return err
			},
			func(err error) {
				// Scrape manager needs to be stopped before closing the local TSDB
				// so that it doesn't try to write samples to a closed storage.
				level.Info(logger).Log("msg", "Stopping scrape manager...")
				scrapeManager.Stop()
			},
		)
	}
	{
		// Reload handler.

		// Make sure that sighup handler is registered with a redirect to the channel before the potentially
		// long and synchronous tsdb init.
		hup := make(chan os.Signal, 1)
		signal.Notify(hup, syscall.SIGHUP)
		cancel := make(chan struct{})
		// 用于系统配置的热加载, reloadConfig 方法用于加载系统配置文件, 且当 Prometheus 进程收到 SIGHUP 信号
		// 或者收到 "curl -X POST localhost:9090/-/reload" 请求(Prometheus 启动参数 --web.enable-lifecycle)
		// 时, reloadConfig 方法会被调用
		g.Add(
			func() error {
				<-reloadReady.C

				for {
					select {
					case <-hup:
						if err := reloadConfig(cfg.configFile, logger, reloaders...); err != nil {
							level.Error(logger).Log("msg", "Error reloading config", "err", err)
						}
					case rc := <-webHandler.Reload():
						if err := reloadConfig(cfg.configFile, logger, reloaders...); err != nil {
							level.Error(logger).Log("msg", "Error reloading config", "err", err)
							rc <- err
						} else {
							rc <- nil
						}
					case <-cancel:
						return nil
					}
				}

			},
			func(err error) {
				// Wait for any in-progress reloads to complete to avoid
				// reloading things after they have been shutdown.
				cancel <- struct{}{}
			},
		)
	}
	{
		// 用于初始化系统配置的参数和设置 Web 可用的服务状态
		// Initial configuration loading.
		cancel := make(chan struct{})
		g.Add(
			func() error {
				select {
				case <-dbOpen:
					break
				// In case a shutdown is initiated before the dbOpen is released
				case <-cancel:
					reloadReady.Close()
					return nil
				}

				if err := reloadConfig(cfg.configFile, logger, reloaders...); err != nil {
					return errors.Wrapf(err, "error loading config from %q", cfg.configFile)
				}

				reloadReady.Close()

				webHandler.Ready()
				level.Info(logger).Log("msg", "Server is ready to receive web requests.")
				<-cancel
				return nil
			},
			func(err error) {
				close(cancel)
			},
		)
	}
	{
		// 启动规则管理组件 ruleManager
		// Rule manager.
		// TODO(krasi) refactor ruleManager.Run() to be blocking to avoid using an extra blocking channel.
		cancel := make(chan struct{})
		g.Add(
			func() error {
				<-reloadReady.C
				ruleManager.Run()
				<-cancel
				return nil
			},
			func(err error) {
				ruleManager.Stop()
				close(cancel)
			},
		)
	}
	{
		// 启动存储组件, Prometheus 指标的存储采用的是时序数据库, 所以在初始化启动
		// 时会设置开始时间和存储路径.
		// TSDB.
		cancel := make(chan struct{})
		g.Add(
			func() error {
				level.Info(logger).Log("msg", "Starting TSDB ...")
				if cfg.tsdb.WALSegmentSize != 0 {
					if cfg.tsdb.WALSegmentSize < 10*1024*1024 || cfg.tsdb.WALSegmentSize > 256*1024*1024 {
						return errors.New("flag 'storage.tsdb.wal-segment-size' must be set between 10MB and 256MB")
					}
				}
				db, err := tsdb.Open(
					cfg.localStoragePath,
					log.With(logger, "component", "tsdb"),
					prometheus.DefaultRegisterer,
					&cfg.tsdb,
				)
				if err != nil {
					return errors.Wrapf(err, "opening storage failed")
				}
				level.Info(logger).Log("fs_type", prom_runtime.Statfs(cfg.localStoragePath))
				level.Info(logger).Log("msg", "TSDB started")
				level.Debug(logger).Log("msg", "TSDB options",
					"MinBlockDuration", cfg.tsdb.MinBlockDuration,
					"MaxBlockDuration", cfg.tsdb.MaxBlockDuration,
					"MaxBytes", cfg.tsdb.MaxBytes,
					"NoLockfile", cfg.tsdb.NoLockfile,
					"RetentionDuration", cfg.tsdb.RetentionDuration,
					"WALSegmentSize", cfg.tsdb.WALSegmentSize,
					"AllowOverlappingBlocks", cfg.tsdb.AllowOverlappingBlocks,
					"WALCompression", cfg.tsdb.WALCompression,
				)

				startTimeMargin := int64(2 * time.Duration(cfg.tsdb.MinBlockDuration).Seconds() * 1000)
				localStorage.Set(db, startTimeMargin)
				close(dbOpen)
				<-cancel
				return nil
			},
			func(err error) {
				if err := fanoutStorage.Close(); err != nil {
					level.Error(logger).Log("msg", "Error stopping storage", "err", err)
				}
				close(cancel)
			},
		)
	}
	{
		// 启动 Web 服务组件
		// Web handler.
		g.Add(
			func() error {
				if err := webHandler.Run(ctxWeb); err != nil {
					return errors.Wrapf(err, "error starting web server")
				}
				return nil
			},
			func(err error) {
				cancelWeb()
			},
		)
	}
	{
		// Notifier.

		// Calling notifier.Stop() before ruleManager.Stop() will cause a panic if the ruleManager isn't running,
		// so keep this interrupt after the ruleManager.Stop().
		g.Add( // 根据在 discoveryManagerNotify 发现的 AlertManager 启动 notifier 组件服务
			func() error {
				// When the notifier manager receives a new targets list
				// it needs to read a valid config for each job.
				// It depends on the config being in sync with the discovery manager
				// so we wait until the config is fully loaded.
				<-reloadReady.C

				notifierManager.Run(discoveryManagerNotify.SyncCh())
				level.Info(logger).Log("msg", "Notifier manager stopped")
				return nil
			},
			func(err error) {
				notifierManager.Stop()
			},
		)
	}
	// 在完成各组件服务启动方法的添加后, 调用 Run() 方法运行
	// execute 方法在 Run 方法中先被执行, 当 execute 方法在执行过程中出现错误或者退出时,
	// interrupt 方法将被触发执行, 且加入 actors 中的所有 interrupt 方法都将被触发.
	if err := g.Run(); err != nil {
		level.Error(logger).Log("err", err)
		os.Exit(1)
	}
	level.Info(logger).Log("msg", "See you next time!")
}

func reloadConfig(filename string, logger log.Logger, rls ...func(*config.Config) error) (err error) {
	level.Info(logger).Log("msg", "Loading configuration file", "filename", filename)

	defer func() {
		if err == nil {
			configSuccess.Set(1)
			configSuccessTime.SetToCurrentTime()
		} else {
			configSuccess.Set(0)
		}
	}()

	conf, err := config.LoadFile(filename)
	if err != nil {
		return errors.Wrapf(err, "couldn't load configuration (--config.file=%q)", filename)
	}

	failed := false
	for _, rl := range rls {
		if err := rl(conf); err != nil {
			level.Error(logger).Log("msg", "Failed to apply configuration", "err", err)
			failed = true
		}
	}
	if failed {
		return errors.Errorf("one or more errors occurred while applying the new configuration (--config.file=%q)", filename)
	}

	promql.SetDefaultEvaluationInterval(time.Duration(conf.GlobalConfig.EvaluationInterval))
	level.Info(logger).Log("msg", "Completed loading of configuration file", "filename", filename)
	return nil
}

func startsOrEndsWithQuote(s string) bool {
	return strings.HasPrefix(s, "\"") || strings.HasPrefix(s, "'") ||
		strings.HasSuffix(s, "\"") || strings.HasSuffix(s, "'")
}

// compileCORSRegexString compiles given string and adds anchors
func compileCORSRegexString(s string) (*regexp.Regexp, error) {
	r, err := relabel.NewRegexp(s)
	if err != nil {
		return nil, err
	}
	return r.Regexp, nil
}

// computeExternalURL computes a sanitized external URL from a raw input. It infers unset
// URL parts from the OS and the given listen address.
func computeExternalURL(u, listenAddr string) (*url.URL, error) {
	if u == "" {
		hostname, err := os.Hostname()
		if err != nil {
			return nil, err
		}
		_, port, err := net.SplitHostPort(listenAddr)
		if err != nil {
			return nil, err
		}
		u = fmt.Sprintf("http://%s:%s/", hostname, port)
	}

	if startsOrEndsWithQuote(u) {
		return nil, errors.New("URL must not begin or end with quotes")
	}

	eu, err := url.Parse(u)
	if err != nil {
		return nil, err
	}

	ppref := strings.TrimRight(eu.Path, "/")
	if ppref != "" && !strings.HasPrefix(ppref, "/") {
		ppref = "/" + ppref
	}
	eu.Path = ppref

	return eu, nil
}

type sender interface {
	Send(alerts ...*notifier.Alert)
}

// sendAlerts implements the rules.NotifyFunc for a Notifier.
func sendAlerts(s sender, externalURL string) rules.NotifyFunc {
	return func(ctx context.Context, expr string, alerts ...*rules.Alert) {
		var res []*notifier.Alert

		for _, alert := range alerts {
			a := &notifier.Alert{
				StartsAt:     alert.FiredAt,
				Labels:       alert.Labels,
				Annotations:  alert.Annotations,
				GeneratorURL: externalURL + strutil.TableLinkForExpression(expr),
			}
			if !alert.ResolvedAt.IsZero() {
				a.EndsAt = alert.ResolvedAt
			} else {
				a.EndsAt = alert.ValidUntil
			}
			res = append(res, a)
		}

		if len(alerts) > 0 {
			s.Send(res...)
		}
	}
}
