package tests

import (
	"context"
	"fmt"
	"sync/atomic"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
)

func registerCreateUserConcurrent(r registry.Registry) {
	runPgx := func(
		ctx context.Context,
		t test.Test,
		c cluster.Cluster,
	) {
		t.Status("setting up cockroach")
		c.Put(ctx, t.Cockroach(), "./cockroach", c.All())
		c.Start(ctx, c.All())

		roundRobinRunner := sqlutils.MakeRoundRobinSQLRunner(
			c.Conn(ctx, 1),
			c.Conn(ctx, 2),
			c.Conn(ctx, 3),
			c.Conn(ctx, 4),
			c.Conn(ctx, 5),
			c.Conn(ctx, 6),
			c.Conn(ctx, 7),
			c.Conn(ctx, 8),
			c.Conn(ctx, 9),
			c.Conn(ctx, 10),
		)

		done := make(chan bool, 1)
		var ops uint64
		for i := 0; i < 5000; i++ {
			go func() {
				if i%2 == 0 {
					roundRobinRunner.Exec(t, "CREATE USER IF NOT EXISTS testuser")
				} else {
					roundRobinRunner.Exec(t, "DROP USER IF EXISTS testuser")
				}
				atomic.AddUint64(&ops, 1)
				fmt.Println(ops)
				if ops == 5000 {
					done <- true
				}
			}()
		}
		<-done
	}

	r.Add(registry.TestSpec{
		Name:    "createuserconcurrent",
		Owner:   registry.OwnerSQLExperience,
		Cluster: r.MakeClusterSpec(10),
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			runPgx(ctx, t, c)
		},
	})
}
