package chanevents

import (
	"context"
	"database/sql"
	"errors"
	"time"

	"github.com/btcsuite/btcd/btcutil"
	"github.com/lightninglabs/faraday/db/sqlc"
	"github.com/lightningnetwork/lnd/clock"
	"github.com/lightningnetwork/lnd/fn/v2"
	"github.com/lightningnetwork/lnd/sqldb/v2"
)

var (
	errUnknownPeer    = errors.New("unknown peer")
	errUnknownChannel = errors.New("unknown channel")
)

// PeerQueries is a subset of the sqlc.Queries interface that can be used
// to interact with the peers, channels and channel_events tables.
type PeerQueries interface {
	UpsertPeer(ctx context.Context, arg sqlc.UpsertPeerParams) (int64, error)
	GetPeerByPubKey(ctx context.Context, pubkey string) (sqlc.Peer, error)
	InsertChannel(ctx context.Context, arg sqlc.InsertChannelParams) (int64, error)
	GetChannelByChanPoint(ctx context.Context, channelPoint string) (sqlc.Channel, error)
	GetChannelByShortChanID(ctx context.Context, shortChannelID sql.NullInt64) (sqlc.Channel, error)
	UpdateShortChanID(ctx context.Context, arg sqlc.UpdateShortChanIDParams) error
	InsertChannelEvent(ctx context.Context, arg sqlc.InsertChannelEventParams) error
	GetChannelEvents(ctx context.Context, arg sqlc.GetChannelEventsParams) ([]sqlc.ChannelEvent, error)
}

// Store represents gives access to the db for channel events.
type Store struct {
	// db is all the higher level queries that the SQLStore has access to
	// in order to implement all its CRUD logic.
	db BatchedSQLQueries

	// BaseDB represents the underlying database connection.
	*sqldb.BaseDB

	clock clock.Clock
}

// BatchedSQLQueries combines the SQLQueries interface with the BatchedTx
// interface, allowing for multiple queries to be executed in single SQL
// transaction.
type BatchedSQLQueries interface {
	SQLQueries

	sqldb.BatchedTx[SQLQueries]
}

// SQLQueries is a subset of the sqlc.Queries interface that can be used to
// interact with various firewalldb tables.
type SQLQueries interface {
	sqldb.BaseQuerier

	PeerQueries
}

type SQLQueriesExecutor[T sqldb.BaseQuerier] struct {
	*sqldb.TransactionExecutor[T]

	SQLQueries
}

// NewStore creates a new SQLStore instance given an open SQLQueries
// storage backend.
func NewStore(sqlDB *sqldb.BaseDB, queries *sqlc.Queries,
	clock clock.Clock) *Store {

	txExecutor := sqldb.NewTransactionExecutor(
		sqlDB, func(tx *sql.Tx) SQLQueries {
			return queries.WithTx(tx)
		},
	)

	executor := &SQLQueriesExecutor[SQLQueries]{
		TransactionExecutor: txExecutor,
		SQLQueries:          queries,
	}

	return &Store{
		db:     executor,
		BaseDB: sqlDB,
		clock:  clock,
	}
}

// AddPeer adds a new peer to the database.
func (s *Store) AddPeer(ctx context.Context, peer *Peer) (int64, error) {
	var alias sql.NullString
	peer.Alias.WhenSome(func(a string) {
		alias.String = a
		alias.Valid = true
	})

	id, err := s.db.UpsertPeer(ctx, sqlc.UpsertPeerParams{
		Pubkey: peer.PubKey,
		Alias:  alias,
	})
	if err != nil {
		return 0, err
	}

	return id, nil
}

// GetPeer retrieves a peer by their public key.
func (s *Store) GetPeer(ctx context.Context, pubkey string) (*Peer, error) {
	dbPeer, err := s.db.GetPeerByPubKey(ctx, pubkey)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, errUnknownPeer
		}
		return nil, err
	}

	var alias fn.Option[string]
	if dbPeer.Alias.Valid {
		alias = fn.Some(dbPeer.Alias.String)
	}

	return &Peer{
		ID:     dbPeer.ID,
		PubKey: dbPeer.Pubkey,
		Alias:  alias,
	}, nil
}

// AddChannel adds a new channel for a peer.
func (s *Store) AddChannel(ctx context.Context, channel *Channel) (int64, error) {
	var shortChanID sql.NullInt64
	channel.ShortChannelID.WhenSome(func(scid int64) {
		shortChanID.Int64 = scid
		shortChanID.Valid = true
	})

	id, err := s.db.InsertChannel(ctx, sqlc.InsertChannelParams{
		ChannelPoint:   channel.ChannelPoint,
		ShortChannelID: shortChanID,
		PeerID:         channel.PeerID,
	})
	if err != nil {
		return 0, err
	}

	return id, nil
}

// GetChannel retrieves a channel by its channel point.
func (s *Store) GetChannel(ctx context.Context, channelPoint string) (*Channel, error) {
	dbChannel, err := s.db.GetChannelByChanPoint(ctx, channelPoint)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, errUnknownChannel
		}
		return nil, err
	}

	var shortChanID fn.Option[int64]
	if dbChannel.ShortChannelID.Valid {
		shortChanID = fn.Some(dbChannel.ShortChannelID.Int64)
	}

	return &Channel{
		ID:             dbChannel.ID,
		ChannelPoint:   dbChannel.ChannelPoint,
		ShortChannelID: shortChanID,
		PeerID:         dbChannel.PeerID,
	}, nil
}

// UpdateChannel updates a channel's short channel ID.
func (s *Store) UpdateChannel(ctx context.Context, channelID int64, shortChannelID int64) error {
	return s.db.UpdateShortChanID(ctx, sqlc.UpdateShortChanIDParams{
		ID:             channelID,
		ShortChannelID: sql.NullInt64{Int64: shortChannelID, Valid: true},
	})
}

// AddChannelEvent adds a new channel event.
func (s *Store) AddChannelEvent(ctx context.Context, event *ChannelEvent) error {
	var localBalance sql.NullInt64
	event.LocalBalance.WhenSome(func(b btcutil.Amount) {
		localBalance.Int64 = int64(b)
		localBalance.Valid = true
	})

	var remoteBalance sql.NullInt64
	event.RemoteBalance.WhenSome(func(b btcutil.Amount) {
		remoteBalance.Int64 = int64(b)
		remoteBalance.Valid = true
	})

	return s.db.InsertChannelEvent(ctx, sqlc.InsertChannelEventParams{
		ChannelID:        event.ChannelID,
		EventType:        event.EventType.String(),
		Timestamp:        s.clock.Now().UTC(),
		LocalBalanceSat:  localBalance,
		RemoteBalanceSat: remoteBalance,
	})
}

// GetChannelEvents retrieves all events for a channel within a given time range.
func (s *Store) GetChannelEvents(ctx context.Context, channelID int64,
	startTime, endTime time.Time) ([]*ChannelEvent, error) {

	dbEvents, err := s.db.GetChannelEvents(ctx, sqlc.GetChannelEventsParams{
		ChannelID:   channelID,
		Timestamp:   startTime.UTC(),
		Timestamp_2: endTime.UTC(),
	})
	if err != nil {
		return nil, err
	}

	events := make([]*ChannelEvent, len(dbEvents))
	for i, dbEvent := range dbEvents {
		var localBalance fn.Option[btcutil.Amount]
		if dbEvent.LocalBalanceSat.Valid {
			amt := btcutil.Amount(dbEvent.LocalBalanceSat.Int64)
			localBalance = fn.Some(amt)
		}

		var remoteBalance fn.Option[btcutil.Amount]
		if dbEvent.RemoteBalanceSat.Valid {
			amt := btcutil.Amount(dbEvent.RemoteBalanceSat.Int64)
			remoteBalance = fn.Some(amt)
		}

		events[i] = &ChannelEvent{
			ID:            dbEvent.ID,
			ChannelID:     dbEvent.ChannelID,
			EventType:     EventTypeFromString(dbEvent.EventType),
			Timestamp:     dbEvent.Timestamp.UTC(),
			LocalBalance:  localBalance,
			RemoteBalance: remoteBalance,
		}
	}

	return events, nil
}
