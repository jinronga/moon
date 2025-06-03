package rpc

import (
	"context"

	"github.com/go-kratos/kratos/v2/log"

	"github.com/aide-family/moon/cmd/palace/internal/biz/bo"
	"github.com/aide-family/moon/cmd/palace/internal/biz/repository"
	"github.com/aide-family/moon/cmd/palace/internal/biz/vobj"
	"github.com/aide-family/moon/cmd/palace/internal/data"
	"github.com/aide-family/moon/pkg/api/common"
	houyiv1 "github.com/aide-family/moon/pkg/api/houyi/v1"
	"github.com/aide-family/moon/pkg/config"
	"github.com/aide-family/moon/pkg/merr"
	"github.com/aide-family/moon/pkg/util/validate"
)

func NewHouyiServer(data *data.Data, logger log.Logger) repository.Houyi {
	return &houyiServer{
		Data:   data,
		helper: log.NewHelper(log.With(logger, "module", "data.repo.houyi")),
	}
}

type houyiServer struct {
	*data.Data
	helper *log.Helper
}

func (s *houyiServer) PushStrategy() (repository.HouyiPushClient, bool) {
	servers, ok := s.ServerConList(vobj.ServerTypeHouyi)
	if !ok {
		return nil, false
	}
	return &houyiPushClient{
		servers: servers,
		helper:  s.helper,
	}, true
}

type houyiSyncClient struct {
	server *bo.Server
}

func (s *houyiServer) Sync() (repository.HouyiSyncClient, bool) {
	server, ok := s.FirstServerConn(vobj.ServerTypeHouyi)
	if !ok {
		return nil, false
	}
	return &houyiSyncClient{server: server}, true
}

func (s *houyiSyncClient) MetricMetadata(ctx context.Context, req *houyiv1.MetricMetadataRequest) (*houyiv1.SyncReply, error) {
	switch s.server.Config.Server.GetNetwork() {
	case config.Network_GRPC:
		return houyiv1.NewSyncClient(s.server.Conn).MetricMetadata(ctx, req)
	case config.Network_HTTP:
		return houyiv1.NewSyncHTTPClient(s.server.Client).MetricMetadata(ctx, req)
	default:
		return nil, merr.ErrorInternalServer("network is not supported")
	}
}

type houyiQueryClient struct {
	server *bo.Server
}

func (s *houyiServer) Query() (repository.HouyiQueryClient, bool) {
	server, ok := s.FirstServerConn(vobj.ServerTypeHouyi)
	if !ok {
		return nil, false
	}
	return &houyiQueryClient{server: server}, true
}

func (s *houyiQueryClient) MetricDatasourceQuery(ctx context.Context, req *houyiv1.MetricDatasourceQueryRequest) (*common.MetricDatasourceQueryReply, error) {
	switch s.server.Config.Server.GetNetwork() {
	case config.Network_GRPC:
		return houyiv1.NewQueryClient(s.server.Conn).MetricDatasourceQuery(ctx, req)
	case config.Network_HTTP:
		return houyiv1.NewQueryHTTPClient(s.server.Client).MetricDatasourceQuery(ctx, req)
	default:
		return nil, merr.ErrorInternalServer("network is not supported")
	}
}

type houyiPushClient struct {
	servers map[string]*bo.Server
	helper  *log.Helper
}

func (s *houyiPushClient) PushStrategy(ctx context.Context, req *houyiv1.PushStrategyRequest) (*houyiv1.PushStrategyReply, error) {
	s.helper.Infof("PushStrategy: %v", req)
	for _, server := range s.servers {
		if err := s.sendStrategy(ctx, server, req); err != nil {
			s.helper.Errorf("PushStrategy error: %v", err)
			continue
		}
	}
	return &houyiv1.PushStrategyReply{
		Code:    0,
		Message: "success",
	}, nil
}

func (s *houyiPushClient) sendStrategy(ctx context.Context, server *bo.Server, req *houyiv1.PushStrategyRequest) error {
	switch server.Config.Server.GetNetwork() {
	case config.Network_GRPC:

		if validate.IsNotNil(req.GetMetric()) {
			if _, err := houyiv1.NewSyncClient(server.Conn).MetricStrategy(ctx, req.GetMetric()); err != nil {
				return err
			}
		}

		if validate.IsNotNil(req.GetCertificate()) {
			if _, err := houyiv1.NewSyncClient(server.Conn).CertificateStrategy(ctx, req.GetCertificate()); err != nil {
				return err
			}
		}

		if validate.IsNotNil(req.GetLogs()) {
			if _, err := houyiv1.NewSyncClient(server.Conn).LogsStrategy(ctx, req.GetLogs()); err != nil {
				return err
			}
		}

		if validate.IsNotNil(req.GetEvent()) {
			if _, err := houyiv1.NewSyncClient(server.Conn).EventStrategy(ctx, req.GetEvent()); err != nil {
				return err
			}
		}

		if validate.IsNotNil(req.GetHttp()) {
			if _, err := houyiv1.NewSyncClient(server.Conn).HttpStrategy(ctx, req.GetHttp()); err != nil {
				return err
			}
		}

		if validate.IsNotNil(req.GetPing()) {
			if _, err := houyiv1.NewSyncClient(server.Conn).PingStrategy(ctx, req.GetPing()); err != nil {
				return err
			}
		}

		if validate.IsNotNil(req.GetServerPort()) {
			if _, err := houyiv1.NewSyncClient(server.Conn).ServerPortStrategy(ctx, req.GetServerPort()); err != nil {
				return err
			}
		}

	case config.Network_HTTP:
		if validate.IsNotNil(req.GetMetric()) {
			if _, err := houyiv1.NewSyncHTTPClient(server.Client).MetricStrategy(ctx, req.GetMetric()); err != nil {
				return err
			}
		}

		if validate.IsNotNil(req.GetCertificate()) {
			if _, err := houyiv1.NewSyncHTTPClient(server.Client).CertificateStrategy(ctx, req.GetCertificate()); err != nil {
				return err
			}
		}

		if validate.IsNotNil(req.GetLogs()) {
			if _, err := houyiv1.NewSyncHTTPClient(server.Client).LogsStrategy(ctx, req.GetLogs()); err != nil {
				return err
			}
		}

		if validate.IsNotNil(req.GetEvent()) {
			if _, err := houyiv1.NewSyncHTTPClient(server.Client).EventStrategy(ctx, req.GetEvent()); err != nil {
				return err
			}
		}

		if validate.IsNotNil(req.GetHttp()) {
			if _, err := houyiv1.NewSyncHTTPClient(server.Client).HttpStrategy(ctx, req.GetHttp()); err != nil {
				return err
			}
		}

		if validate.IsNotNil(req.GetPing()) {
			if _, err := houyiv1.NewSyncHTTPClient(server.Client).PingStrategy(ctx, req.GetPing()); err != nil {
				return err
			}
		}

		if validate.IsNotNil(req.GetServerPort()) {
			if _, err := houyiv1.NewSyncHTTPClient(server.Client).ServerPortStrategy(ctx, req.GetServerPort()); err != nil {
				return err
			}
		}
	default:
		return merr.ErrorInternalServer("network is not supported")
	}
	return nil
}
