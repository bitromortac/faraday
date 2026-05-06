// Package mock provides testify-based fakes for frdrpcserver dependencies.
//
// Co-locating mocks under the package that defines their interfaces keeps
// the contract and the fake adjacent — any consumer importing the
// interface inherently knows where to find the canonical mock.
package mock

import (
	"context"
	"time"

	"github.com/btcsuite/btcd/btcutil"
	"github.com/lightninglabs/faraday/chanevents"
	"github.com/stretchr/testify/mock"
)

// ForwardingAnalyzer is a testify-mock fake of the
// frdrpcserver.ForwardingAnalyzer interface. Tests stub EffectiveUptime
// to drive the gRPC handler against a known matrix without spinning up
// the chanevents store or lnd.
type ForwardingAnalyzer struct {
	mock.Mock
}

// EffectiveUptime records the call and returns whatever the test stubbed
// via On("EffectiveUptime", ...).Return(matrix, err).
func (m *ForwardingAnalyzer) EffectiveUptime(ctx context.Context, startTime,
	endTime time.Time, fwdPct float64, threshold btcutil.Amount) (
	map[chanevents.PeerPair]chanevents.ForwardingAbility, error) {

	args := m.Called(ctx, startTime, endTime, fwdPct, threshold)
	if abilities := args.Get(0); abilities != nil {
		return abilities.(map[chanevents.PeerPair]chanevents.ForwardingAbility),
			args.Error(1)
	}

	return nil, args.Error(1)
}
