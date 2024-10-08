package rabbit

import (
	"github.com/aide-family/moon/pkg/helper/hello"
	"github.com/aide-family/moon/pkg/util/codec"
	"github.com/aide-family/moon/pkg/util/conn"
	sLog "github.com/aide-family/moon/pkg/util/log"
	"github.com/aide-family/moon/pkg/util/types"

	"github.com/aide-family/moon/cmd/server/rabbit/internal/rabbitconf"
	"github.com/aide-family/moon/cmd/server/rabbit/internal/server"
	"github.com/aide-family/moon/pkg/env"
	"github.com/go-kratos/kratos/v2"
	"github.com/go-kratos/kratos/v2/config"
	"github.com/go-kratos/kratos/v2/config/file"
	"github.com/go-kratos/kratos/v2/encoding/json"
	"github.com/go-kratos/kratos/v2/log"
	"google.golang.org/protobuf/encoding/protojson"
)

func newApp(c *rabbitconf.Bootstrap, srv *server.Server, logger log.Logger) *kratos.App {
	opts := []kratos.Option{
		kratos.ID(env.ID()),
		kratos.Name(env.Name()),
		kratos.Version(env.Version()),
		kratos.Metadata(env.Metadata()),
		kratos.Logger(logger),
		kratos.Server(srv.GetServers()...),
	}
	registerConf := c.GetDiscovery()
	if !types.IsNil(registerConf) {
		register, err := conn.NewRegister(c.GetDiscovery())
		if !types.IsNil(err) {
			log.Warnw("register error", err)
			panic(err)
		}
		opts = append(opts, kratos.Registrar(register))
	}

	return kratos.New(opts...)
}

// Run 启动服务
func Run(flagconf, configType string) {
	// 注册配置文件类型
	codec.RegisterCodec(configType)
	c := config.New(config.WithSource(file.NewSource(flagconf)))
	defer c.Close()

	if err := c.Load(); !types.IsNil(err) {
		panic(err)
	}

	var bc rabbitconf.Bootstrap
	if err := c.Scan(&bc); !types.IsNil(err) {
		panic(err)
	}

	env.SetName(bc.GetServer().GetName())
	env.SetMetadata(bc.GetServer().GetMetadata())
	env.SetEnv(bc.GetEnv())

	logger := sLog.GetLogger()
	log.SetLogger(logger)
	app, cleanup, err := wireApp(&bc, logger)
	if !types.IsNil(err) {
		panic(err)
	}
	defer cleanup()
	hello.Hello()
	// start and wait for stop signal
	if err = app.Run(); !types.IsNil(err) {
		panic(err)
	}
}

func init() {
	//增加这段代码
	json.MarshalOptions = protojson.MarshalOptions{
		UseEnumNumbers: true, // 将枚举值作为数字发出，默认为枚举值的字符串
	}
}
