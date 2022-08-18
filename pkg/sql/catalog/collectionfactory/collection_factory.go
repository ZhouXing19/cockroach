// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package collectionfactory

// DescsCollection is an interface for *descs.Collection.
// It's to avoid cyclic dependency.
type DescsCollection interface {
	ImplmentDescsCollection()
}

// CollectionFactory is an interface for *descs.CollectionFactory.
// It's to avoid cyclic dependency.
type CollectionFactory interface {
	ImplmentCollectionFactory()
}
