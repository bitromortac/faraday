package chanevents

import (
	"testing"

	"github.com/lightninglabs/faraday/db/sqlc"

	"github.com/lightningnetwork/lnd/clock"
	"github.com/lightningnetwork/lnd/sqldb/v2"
	"github.com/stretchr/testify/require"
)

// createStore is a helper function that creates a new SQLDB and ensure that
// it is closed when during the test cleanup.
func createStore(t *testing.T, sqlDB *sqldb.BaseDB, clock clock.Clock) *Store {
	queries := sqlc.NewForType(sqlDB, sqlDB.BackendType)

	store := NewStore(sqlDB, queries, clock)

	t.Cleanup(func() {
		require.NoError(t, store.Close())
	})

	return store
}
