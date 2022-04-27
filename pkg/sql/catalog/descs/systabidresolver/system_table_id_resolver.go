// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package systabidresolver

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs/cftxn"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlutil"
)

// SystemTableIDResolver is the implementation for catalog.SystemTableIDResolver.
type SystemTableIDResolver struct {
	collectionFactory *descs.CollectionFactory
	ie                sqlutil.InternalExecutor
	db                *kv.DB
}

var _ catalog.SystemTableIDResolver = (*SystemTableIDResolver)(nil)

// MakeSystemTableIDResolver creates an object that implements catalog.SystemTableIDResolver.
func MakeSystemTableIDResolver(
	collectionFactory *descs.CollectionFactory, ie sqlutil.InternalExecutor, db *kv.DB,
) catalog.SystemTableIDResolver {
	return &SystemTableIDResolver{
		collectionFactory: collectionFactory,
		ie:                ie,
		db:                db,
	}
}

// LookupSystemTableID implements the catalog.SystemTableIDResolver method.
func (r *SystemTableIDResolver) LookupSystemTableID(
	ctx context.Context, tableName string,
) (descpb.ID, error) {

	var id descpb.ID
	if err := cftxn.CollectionFactoryTxn(ctx, r.collectionFactory, r.ie, r.db, func(
		ctx context.Context, txn *kv.Txn, descriptors *descs.Collection,
	) (err error) {
		id, err = descriptors.LookUpNameInKv(
			ctx, txn, nil /* maybeDatabase */, keys.SystemDatabaseID, keys.PublicSchemaID, tableName,
		)
		return err
	}); err != nil {
		return 0, err
	}
	return id, nil
}
