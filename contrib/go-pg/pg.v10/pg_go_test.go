// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache License Version 2.0.
// This product includes software developed at Datadog (https://www.datadoghq.com/).
// Copyright 2016 Datadog, Inc.

package pg

import (
	"context"
	"fmt"
	"os"
	"testing"

	"github.com/stlimtat/dd-trace-go/ddtrace/ext"
	"github.com/stlimtat/dd-trace-go/ddtrace/mocktracer"
	"github.com/stlimtat/dd-trace-go/ddtrace/tracer"

	"github.com/go-pg/pg/v10"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMain(m *testing.M) {
	_, ok := os.LookupEnv("INTEGRATION")
	if !ok {
		fmt.Println("--- SKIP: to enable integration test, set the INTEGRATION environment variable")
		os.Exit(0)
	}
	os.Exit(m.Run())
}

func TestImplementsHook(t *testing.T) {
	var _ pg.QueryHook = (*queryHook)(nil)
}

func TestSelect(t *testing.T) {
	assert := assert.New(t)
	mt := mocktracer.Start()
	defer mt.Stop()

	conn := pg.Connect(&pg.Options{
		User:     "postgres",
		Database: "postgres",
	})

	Wrap(conn)

	parentSpan, ctx := tracer.StartSpanFromContext(context.Background(), "http.request",
		tracer.ServiceName("fake-http-server"),
		tracer.SpanType(ext.SpanTypeWeb),
	)

	var n int
	// Using WithContext will make the postgres span a child of
	// the span inside ctx (parentSpan)
	res, err := conn.WithContext(ctx).QueryOne(pg.Scan(&n), "SELECT 1")
	parentSpan.Finish()
	spans := mt.FinishedSpans()

	require.NoError(t, err)
	assert.Equal(1, res.RowsAffected())
	assert.Equal(1, res.RowsReturned())
	assert.Equal(2, len(spans))
	assert.Equal(nil, err)
	assert.Equal(1, n)
	assert.Equal("go-pg", spans[0].OperationName())
	assert.Equal("http.request", spans[1].OperationName())
}
