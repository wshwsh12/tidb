package infosync

import (
	"context"

	"github.com/tici/proto/indexer"
)

// TiCIManager manages placement settings and replica progress for TiFlash.
type TiCIManager interface {
	// SetTiFlashGroupConfig sets the group index of the tiflash placement rule
	CreateIndex(ctx context.Context) error
}

// TiCIManagerCtx manages placement with pd and replica progress for TiFlash.
type TiCIManagerCtx struct {
	indexServiceClient indexer.IndexerServiceClient
}

func (t *TiCIManagerCtx) CreateIndex(ctx context.Context) error {
	return nil
}
