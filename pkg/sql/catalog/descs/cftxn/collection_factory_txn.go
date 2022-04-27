// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package cftxn

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs/descsutil"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/lease"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlutil"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/errors"
)

var errTwoVersionInvariantViolated = errors.Errorf("two version invariant violated")

// CollectionFactoryTxn enables callers to run transactions with a *Collection
// such that all retrieved immutable descriptors are properly leased and all
// mutable descriptors are handled. The function deals with verifying the two
// version invariant and retrying when it is violated. Callers need not worry
// that they write mutable descriptors multiple times. The call will explicitly
// wait for the leases to drain on old versions of descriptors modified or
// deleted in the transaction; callers do not need to call
// lease.WaitForOneVersion.
//
// The passed transaction is pre-emptively anchored to the system config key on
// the system tenant.
func CollectionFactoryTxn(
	ctx context.Context,
	cf *descs.CollectionFactory,
	ie sqlutil.InternalExecutor,
	db *kv.DB,
	f func(ctx context.Context, txn *kv.Txn, descriptors *descs.Collection) error,
) error {
	// Waits for descriptors that were modified, skipping
	// over ones that had their descriptor wiped.
	waitForDescriptors := func(modifiedDescriptors []lease.IDVersion, deletedDescs catalog.DescriptorIDSet) error {
		// Wait for a single version on leased descriptors.
		for _, ld := range modifiedDescriptors {
			waitForNoVersion := deletedDescs.Contains(ld.ID)
			// Detect unpublished ones.
			if waitForNoVersion {
				err := cf.GetLeaseManager().WaitForNoVersion(ctx, ld.ID, retry.Options{})
				if err != nil {
					return err
				}
			} else {
				_, err := cf.GetLeaseManager().WaitForOneVersion(ctx, ld.ID, retry.Options{})
				if err != nil {
					return err
				}
			}
		}
		return nil
	}
	for {
		var modifiedDescriptors []lease.IDVersion
		var deletedDescs catalog.DescriptorIDSet
		var descsCol descs.Collection
		if err := db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
			modifiedDescriptors = nil
			deletedDescs = catalog.DescriptorIDSet{}
			descsCol = cf.MakeCollection(ctx, nil /* temporarySchemaProvider */, nil /* monitor */)
			defer descsCol.ReleaseAll(ctx)
			if !cf.GetClusterSettings().Version.IsActive(
				ctx, clusterversion.DisableSystemConfigGossipTrigger,
			) {
				if err := txn.DeprecatedSetSystemConfigTrigger(
					cf.GetLeaseManager().Codec().ForSystemTenant(),
				); err != nil {
					return err
				}
			}
			if err := f(ctx, txn, &descsCol); err != nil {
				return err
			}

			if err := descsCol.ValidateUncommittedDescriptors(ctx, txn); err != nil {
				return err
			}
			modifiedDescriptors = descsCol.GetDescriptorsWithNewVersion()

			if err := descsutil.CheckSpanCountLimit(
				ctx, &descsCol, cf.GetSpanConfigSplitter(), cf.GetSpanConfigLimiter(), txn,
			); err != nil {
				return err
			}
			retryErr, err := descsutil.CheckTwoVersionInvariant(
				ctx, db.Clock(), ie, &descsCol, txn, nil /* onRetryBackoff */)
			if retryErr {
				return errTwoVersionInvariantViolated
			}
			deletedDescs = descsCol.GetDeletedDescs()
			return err
		}); errors.Is(err, errTwoVersionInvariantViolated) {
			continue
		} else {
			if err == nil {
				err = waitForDescriptors(modifiedDescriptors, deletedDescs)
			}
			return err
		}
	}
}
