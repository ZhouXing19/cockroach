// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package cli

import (
	"archive/zip"
	"bytes"
	"context"
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cli/clierrorplus"
	"github.com/cockroachdb/cockroach/pkg/cli/clisqlclient"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/errors"
	"github.com/spf13/cobra"
)

var debugStatementBundleCmd = &cobra.Command{
	Use:     "statement-bundle [command]",
	Aliases: []string{"sb"},
	Short:   "run a cockroach debug statement-bundle tool command",
	Long: `
debug statement-bundle is a suite of tools for debugging and manipulating statement
bundles created with EXPLAIN ANALYZE (DEBUG).
`,
}

var statementBundleRecreateCmd = &cobra.Command{
	Use:   "recreate <stmt bundle zipdir>",
	Short: "recreate the statement bundle in a demo cluster",
	Long: `
Run the recreate tool to populate a demo cluster with the environment, schema,
and stats in an unzipped statement bundle directory.
`,
	Args: cobra.ExactArgs(1),
}

var statementBundleAgentCmd = &cobra.Command{
	Use:   "agent <bundle.zip> <question/task> [--binary-path=<path>]",
	Short: "analyze statement bundle and generate hypothesis to answer questions",
	Long: `
The statement bundle agent analyzes CockroachDB statement bundles and generates
hypotheses to answer performance questions or execute diagnostic tasks.

Examples:
  # Analyze query performance
  cockroach debug statement-bundle agent bundle.zip "Why is this query slow?"
  
  # Check for missing indexes
  cockroach debug statement-bundle agent bundle.zip "Are there missing indexes?"
  
  # Analyze resource usage
  cockroach debug statement-bundle agent bundle.zip "What resources does this query use?"
`,
	Args: cobra.ExactArgs(2),
}

var (
	placeholderPairs []string
	explainPrefix    string
	binaryPath       string
	commentPattern   = regexp.MustCompile(`^\s*--`)
)

func init() {
	statementBundleRecreateCmd.RunE = clierrorplus.MaybeDecorateError(runBundleRecreate)

	statementBundleRecreateCmd.Flags().StringArrayVar(&placeholderPairs, "placeholder", nil,
		"pass in a map of placeholder id to fully-qualified table column to get the program to produce all optimal"+
			" of explain plans with each of the histogram values for each column replaced in its placeholder.")
	statementBundleRecreateCmd.Flags().StringVar(&explainPrefix, "explain-cmd", "EXPLAIN",
		"set the EXPLAIN command used to produce the final output when displaying all optimal explain plans with"+
			" --placeholder. Example: EXPLAIN(OPT)")
	
	statementBundleAgentCmd.RunE = clierrorplus.MaybeDecorateError(runBundleAgent)
	statementBundleAgentCmd.Flags().StringVar(&binaryPath, "binary-path", "", 
		"Path to cockroach binary for recreate command (defaults to current binary)")
}

type statementBundle struct {
	env       []byte
	schema    []byte
	statement []byte
	stats     [][]byte
}

func loadStatementBundle(zipdir string) (*statementBundle, error) {
	ret := &statementBundle{}
	var err error
	ret.env, err = os.ReadFile(filepath.Join(zipdir, "env.sql"))
	if err != nil {
		return ret, err
	}
	ret.schema, err = os.ReadFile(filepath.Join(zipdir, "schema.sql"))
	if err != nil {
		return ret, err
	}
	ret.statement, err = os.ReadFile(filepath.Join(zipdir, "statement.sql"))
	if err != nil {
		// In 21.2 and prior releases, the statement file had 'txt' extension,
		// let's try that.
		var newErr error
		ret.statement, newErr = os.ReadFile(filepath.Join(zipdir, "statement.txt"))
		if newErr != nil {
			return ret, errors.CombineErrors(err, newErr)
		}
	}

	return ret, filepath.WalkDir(zipdir, func(path string, d fs.DirEntry, _ error) error {
		if d.IsDir() {
			return nil
		}
		if !strings.HasPrefix(d.Name(), "stats-") {
			return nil
		}
		f, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		ret.stats = append(ret.stats, f)
		return nil
	})
}

func runBundleRecreate(cmd *cobra.Command, args []string) (resErr error) {
	zipdir := args[0]
	bundle, err := loadStatementBundle(zipdir)
	if err != nil {
		return err
	}

	var demoLocalityInfo string
	schema := string(bundle.schema)
	if strings.Contains(schema, "REGIONS = ") {
		// We have at least one multi-region DB, so we'll extract all regions.
		regions := make(map[string]struct{})
		for _, line := range strings.Split(schema, "\n") {
			line = strings.TrimSpace(line)
			if strings.HasPrefix(line, "CREATE DATABASE") {
				stmt, err := parser.ParseOne(line)
				if err != nil {
					return err
				}
				createDB, ok := stmt.AST.(*tree.CreateDatabase)
				if !ok {
					return errors.Errorf("expected *tree.CreateDatabase AST node but got %T for %s", stmt.AST, line)
				}
				for _, region := range createDB.Regions.ToStrings() {
					if _, ok = regions[region]; !ok {
						regions[region] = struct{}{}
					}
				}
			}
		}
		if len(regions) > 0 {
			// Every region will get exactly one node.
			demoCtx.NumNodes = len(regions)
			var regionsString string
			for region := range regions {
				demoCtx.Localities = append(demoCtx.Localities, roachpb.Locality{
					Tiers: []roachpb.Tier{
						{Key: "region", Value: region},
						{Key: "az", Value: "1"}, // this shouldn't matter
					},
				})
				if regionsString != "" {
					regionsString += `, "` + region + `"`
				} else {
					regionsString += `"` + region + `"`
				}
			}
			demoLocalityInfo = fmt.Sprintf("\n# Started %d nodes with regions %s.", len(regions), regionsString)
		}
	}

	demoCtx.UseEmptyDatabase = true
	demoCtx.Multitenant = false
	return runDemoInternal(cmd, nil /* gen */, func(ctx context.Context, conn clisqlclient.Conn) error {
		// SET CLUSTER SETTING statements cannot be executed in multi-statement
		// implicit transaction, so we need to separate them out into their own
		// implicit transactions. Comments are stripped from the env file first.
		lines := strings.Split(string(bundle.env), "\n")
		lines = slices.DeleteFunc(lines, commentPattern.MatchString)
		initStmts := strings.Split(strings.Join(lines, "\n"), "SET CLUSTER SETTING")
		for i := 1; i < len(initStmts); i++ {
			initStmts[i] = "SET CLUSTER SETTING " + initStmts[i]
			if strings.Contains(initStmts[i], "cluster.preserve_downgrade_option") {
				// This cluster setting can prevent the bundle from being
				// recreated on a new enough binary, so we'll skip it.
				initStmts = append(initStmts[:i], initStmts[i+1:]...)
				i--
			}
		}
		// All stmts before the first SET CLUSTER SETTING are SET stmts. We need
		// to handle 'SET database = ' stmt separately if found - the target
		// database might not exist yet.
		setStmts := strings.Split(initStmts[0], "\n")
		initStmts = initStmts[1:]
		var setDBStmt string
		for i, stmt := range setStmts {
			stmt = strings.TrimSpace(stmt)
			if strings.HasPrefix(stmt, "SET database = ") {
				setDBStmt = stmt
				setStmts = append(setStmts[:i], setStmts[i+1:]...)
				break
			}
		}
		initStmts = append(initStmts, setStmts...)
		// 'default_transaction_use_follower_reads' session variable can break
		// recreation of the bundle, so if we see it being set, we'll also add
		// the corresponding RESET statement.
		for _, stmt := range setStmts {
			stmt = strings.TrimSpace(stmt)
			if strings.HasPrefix(stmt, "SET default_transaction_use_follower_reads = ") {
				initStmts = append(initStmts, "RESET default_transaction_use_follower_reads; -- added by 'recreate'")
				break
			}
		}
		// Disable auto stats collection (which would override the injected
		// stats).
		initStmts = append(initStmts, "SET CLUSTER SETTING sql.stats.automatic_collection.enabled = false;")
		initStmts = append(initStmts, string(bundle.schema))
		if setDBStmt != "" {
			initStmts = append(initStmts, setDBStmt)
		}
		for _, stats := range bundle.stats {
			initStmts = append(initStmts, string(stats))
		}
		for _, s := range initStmts {
			if err := conn.Exec(ctx, s); err != nil {
				return errors.Wrapf(err, "failed to run: %s", s)
			}
		}

		stmt, numPlaceholders, err := sql.ReplacePlaceholdersWithValuesForBundle(string(bundle.statement))
		if err != nil {
			return errors.Wrap(err, "failed to replace placeholders")
		}
		var placeholderInfo string
		if numPlaceholders > 0 {
			var plural string
			if numPlaceholders > 1 {
				plural = "s"
			}
			placeholderInfo = fmt.Sprintf("(had %d placeholder%s) ", numPlaceholders, plural)
		}

		cliCtx.PrintfUnlessEmbedded(`#
# Statement bundle %s loaded.%s
# Autostats disabled.
#
# Statement %swas:
#
# %s
`, zipdir, demoLocalityInfo, placeholderInfo, stmt+";")

		if placeholderPairs != nil {
			placeholderToColMap := make(map[int]string)
			placeholderFQColNames := make(map[string]struct{})
			for _, placeholderPairStr := range placeholderPairs {
				pair := strings.Split(placeholderPairStr, "=")
				if len(pair) != 2 {
					return errors.New("use --placeholder='1=schema.table.col' --placeholder='2=schema.table.col...'")
				}
				n, err := strconv.Atoi(pair[0])
				if err != nil {
					return err
				}
				placeholderToColMap[n] = pair[1]
				placeholderFQColNames[pair[1]] = struct{}{}
			}
			inputs, outputs, err := getExplainCombinations(
				ctx, conn, explainPrefix, placeholderToColMap, placeholderFQColNames, bundle,
			)
			if err != nil {
				return err
			}

			cliCtx.PrintfUnlessEmbedded("found %d unique explains:\n\n", len(inputs))
			for i, inputs := range inputs {
				cliCtx.PrintfUnlessEmbedded("Values %s: \n%s\n----\n\n", inputs, outputs[i])
			}
		}

		return nil
	})
}

// placeholderRe matches the placeholder format at the bottom of statement.txt
// in a statement bundle. It looks like this:
//
// $1: blah
// $2: 1
var placeholderRe = regexp.MustCompile(`\$(\d+): .*`)

// The double quotes are needed for table names that are reserved keywords.
var statsRe = regexp.MustCompile(`ALTER TABLE ([\w".]+) INJECT STATISTICS '`)

type bucketKey struct {
	NumEq         float64
	NumRange      float64
	DistinctRange float64
}

// getExplainCombinations finds all unique optimal explain plans for a given statement
// bundle that are produced by creating all combinations of plans where each
// placeholder is replaced by every value in the column histogram for a linked
// column.
//
// explainPrefix is the type of EXPLAIN to use for the final output, like
// EXPLAIN(OPT).
//
// A list of unique inputs is returned, which corresponds 1 to 1 with the list
// of explain outputs: the ith set of inputs is the set of placeholders that
// produced the ith explain output.
//
// Columns are linked to placeholders by the --placeholder=n=schema.table.col
// commandline flags.
func getExplainCombinations(
	ctx context.Context,
	conn clisqlclient.Conn,
	explainPrefix string,
	placeholderToColMap map[int]string,
	placeholderFQColNames map[string]struct{},
	bundle *statementBundle,
) (inputs [][]string, explainOutputs []string, err error) {

	stmtComponents := strings.Split(string(bundle.statement), "Arguments:")
	statement := strings.TrimSpace(stmtComponents[0])
	placeholders := stmtComponents[1]

	var stmtPlaceholders []int
	for _, line := range strings.Split(placeholders, "\n") {
		// The placeholderRe has 1 matching group, so the length of the matches
		// list will be 2 if we see a successful match.
		if matches := placeholderRe.FindStringSubmatch(line); len(matches) == 2 {
			// The first matching group is the number of the placeholder. Extract it
			// into an integer.
			n, err := strconv.Atoi(matches[1])
			if err != nil {
				return nil, nil, err
			}
			stmtPlaceholders = append(stmtPlaceholders, n)
		}
	}

	for _, n := range stmtPlaceholders {
		if placeholderToColMap[n] == "" {
			return nil, nil, errors.Errorf("specify --placeholder= for placeholder %d", n)
		}
	}
	evalCtx := eval.MakeTestingEvalContext(cluster.MakeTestingClusterSettings())

	fmtCtx := tree.FmtBareStrings

	// Map from fully-qualified column name to list of histogram upper_bound
	// values with unique bucket attributes.
	statsMap := make(map[string][]string)
	statsAge := make(map[string]time.Time)
	for _, statsBytes := range bundle.stats {
		statsStr := string(statsBytes)
		matches := statsRe.FindStringSubmatch(statsStr)
		if len(matches) != 2 {
			return nil, nil, errors.Errorf("invalid stats file %s", statsStr)
		}
		tableName := matches[1]
		// Find the first instance of ', which is the beginning of the JSON payload.
		idx := bytes.IndexByte(statsBytes, '\'')
		var statsJSON []map[string]interface{}

		// Snip off the last 3 characters, which are ';\n, the end of the JSON payload.
		data := statsBytes[idx+1 : len(statsBytes)-3]
		if err := json.Unmarshal(data, &statsJSON); err != nil {
			return nil, nil, err
		}

		// Map a bucket key (a bucket without upper bound) to an upper bound sample
		// and its Prev value (for non-0 width buckets).
		// This deduplicates identical buckets.
		for _, stat := range statsJSON {
			bucketMap := make(map[bucketKey][]string)
			columns := stat["columns"].([]interface{})
			if len(columns) > 1 {
				// Ignore multi-col stats.
				continue
			}
			col := columns[0]
			fqColName := fmt.Sprintf("%s.%s", tableName, col)
			if _, isPlaceholder := placeholderFQColNames[fqColName]; !isPlaceholder {
				// This column is not one of the placeholder values, so simply
				// ignore it.
				continue
			}
			d, _, err := tree.ParseDTimestamp(nil, stat["created_at"].(string), time.Microsecond)
			if err != nil {
				panic(err)
			}
			if lastStat, ok := statsAge[fqColName]; ok && d.Before(lastStat) {
				// Skip stats that are older than the most recent stat.
				continue
			}
			statsAge[fqColName] = d.Time

			typ := stat["histo_col_type"].(string)
			if typ == "" {
				// Empty 'histo_col_type' is used when there is no histogram for
				// the column, simply skip this stat (see stats/json.go for more
				// details).
				continue
			}
			colTypeRef, err := parser.GetTypeFromValidSQLSyntax(typ)
			if err != nil {
				return nil, nil, errors.Wrapf(err, "unable to parse type %s for col %s", typ, col)
			}
			colType := tree.MustBeStaticallyKnownType(colTypeRef)
			if stat["histo_buckets"] == nil {
				// There might not be any histogram buckets if the stats were
				// collected when the table was empty or all values in the
				// column were NULL.
				continue
			}
			buckets := stat["histo_buckets"].([]interface{})
			// addedNonExistent tracks whether we included at least one
			// "previous" datum which - according to the histograms - is not
			// present in the table.
			var addedNonExistent bool
			var maxUpperBound tree.Datum
			for _, b := range buckets {
				bucket := b.(map[string]interface{})
				numRange := bucket["num_range"].(float64)
				key := bucketKey{
					NumEq:         bucket["num_eq"].(float64),
					NumRange:      numRange,
					DistinctRange: bucket["distinct_range"].(float64),
				}
				upperBound := bucket["upper_bound"].(string)
				bucketMap[key] = []string{upperBound}
				datum, err := rowenc.ParseDatumStringAs(ctx, colType, upperBound, &evalCtx, nil /* semaCtx */)
				if err != nil {
					panic("failed parsing datum string as " + colType.String() + " " + err.Error())
				}
				if maxUpperBound == nil {
					maxUpperBound = datum
				} else if cmp, err := maxUpperBound.Compare(ctx, &evalCtx, datum); err != nil {
					panic(err)
				} else if cmp < 0 {
					maxUpperBound = datum
				}
				// If we have any datums within the bucket (i.e. not equal to
				// the upper bound), we always attempt to add a "previous" to
				// the upper bound datum.
				addPrevious := numRange > 0
				if numRange == 0 && !addedNonExistent {
					// If our bucket says that there are no values present in
					// the table between the current upper bound and the upper
					// bound of the previous histogram bucket, then we only
					// attempt to add the "previous" non-existent datum if we
					// haven't done so already (this is to avoid the redundant
					// non-existent values which would get treated in the same
					// fashion anyway).
					addPrevious = true
				}
				if addPrevious {
					if prev, ok := tree.DatumPrev(ctx, datum, &evalCtx, &evalCtx.CollationEnv); ok {
						bucketMap[key] = append(bucketMap[key], tree.AsStringWithFlags(prev, fmtCtx))
						addedNonExistent = addedNonExistent || numRange == 0
					}
				}
			}
			colSamples := make([]string, 0, len(bucketMap))
			for _, samples := range bucketMap {
				colSamples = append(colSamples, samples...)
			}
			// Create a value that's outside of histogram range by incrementing the
			// max value that we've seen.
			if outside, ok := tree.DatumNext(ctx, maxUpperBound, &evalCtx, &evalCtx.CollationEnv); ok {
				colSamples = append(colSamples, tree.AsStringWithFlags(outside, fmtCtx))
			}
			sort.Strings(colSamples)
			statsMap[fqColName] = colSamples
		}
	}

	for _, fqColName := range placeholderToColMap {
		if statsMap[fqColName] == nil {
			return nil, nil, errors.Errorf("no stats found for %s", fqColName)
		}
	}

	combinations := getPlaceholderCombinations(stmtPlaceholders, placeholderToColMap, statsMap)

	outputs, err := getExplainOutputs(conn, "EXPLAIN(SHAPE)", statement, combinations)
	if err != nil {
		return nil, nil, err
	}
	// uniqueExplains maps explain output to the list of placeholders that
	// produced it.
	uniqueExplains := make(map[string][]string)
	for i := range combinations {
		uniqueExplains[outputs[i]] = combinations[i]
	}

	// Sort the explain outputs for consistent results.
	explains := make([]string, 0, len(uniqueExplains))
	for key := range uniqueExplains {
		explains = append(explains, key)
	}
	sort.Strings(explains)

	// Now that we've got the unique explain shapes, re-run them with the desired
	// EXPLAIN style to get sufficient detail.
	uniqueInputs := make([][]string, 0, len(uniqueExplains))
	for _, explain := range explains {
		input := uniqueExplains[explain]
		uniqueInputs = append(uniqueInputs, input)
	}
	outputs, err = getExplainOutputs(conn, explainPrefix, statement, uniqueInputs)
	if err != nil {
		return nil, nil, err
	}

	return uniqueInputs, outputs, nil
}

// getExplainOutputs runs the explain style given in explainPrefix on the
// statement once for every input (an ordered list of placeholder values) in the
// input list. The result is returned in a list of explain outputs, where the
// ith explain output was generated from the ith input.
func getExplainOutputs(
	conn clisqlclient.Conn, explainPrefix string, statement string, inputs [][]string,
) (explainStrings []string, err error) {
	fmt.Printf("trying %d placeholder combinations\n", len(inputs))
	for i, values := range inputs {
		// Run an explain for each possible input.
		query := fmt.Sprintf("%s %s", explainPrefix, statement)
		args := make([]interface{}, len(values))
		for i, s := range values {
			args[i] = s
		}
		rows, err := conn.Query(context.Background(), query, args...)
		if err != nil {
			return nil, err
		}
		row := []driver.Value{""}
		var explainStr = strings.Builder{}
		for err = rows.Next(row); err == nil; err = rows.Next(row) {
			fmt.Fprintln(&explainStr, row[0])
		}
		if err != io.EOF {
			return nil, err
		}
		if err := rows.Close(); err != nil {
			return nil, err
		}
		explainStrings = append(explainStrings, explainStr.String())
		if (i+1)%1000 == 0 {
			fmt.Printf("%d placeholder combinations are done\n", i+1)
		}
	}
	return explainStrings, nil
}

// getPlaceholderCombinations returns a list of lists, which each inner list is
// a possible set of placeholders that can be inserted into the statement, where
// each possible value for each placeholder is taken from the input statsMap.
func getPlaceholderCombinations(
	remainingPlaceholders []int, placeholderMap map[int]string, statsMap map[string][]string,
) [][]string {
	placeholder := remainingPlaceholders[0]
	fqColName := placeholderMap[placeholder]
	var rest = [][]string{nil}
	if len(remainingPlaceholders) > 1 {
		// Recurse to get the rest of the combinations.
		rest = getPlaceholderCombinations(remainingPlaceholders[1:], placeholderMap, statsMap)
	}
	var ret [][]string
	for _, val := range statsMap[fqColName] {
		for _, inner := range rest {
			ret = append(ret, append([]string{val}, inner...))
		}
	}
	return ret
}

// bundleAnalysis contains the parsed contents and analysis of a statement bundle
type bundleAnalysis struct {
	BundlePath   string
	Question     string
	Environment  map[string]string
	Statement    string
	Plans        []string
	Schema       *schemaInfo
	Hypotheses   []hypothesis
}

type schemaInfo struct {
	Tables []tableInfo
}

type tableInfo struct {
	Name    string
	Columns []string
}

type statementInfo struct {
	SQL         string
	Type        string
	Tables      []string
	WhereClause string
	OrderBy     string
	Joins       []joinInfo
}

type joinInfo struct {
	Table     string
	Condition string
}

type hypothesis struct {
	Category     string
	Title        string
	Description  string
	Evidence     []string
	Confidence   float64
	Actions      []string
	SQLSolutions []string // Specific SQL statements to run
}

func runBundleAgent(cmd *cobra.Command, args []string) error {
	bundlePath := args[0]
	question := args[1]

	if binaryPath == "" {
		binaryPath = os.Args[0] // Use current binary by default
	}

	// Extract bundle to temporary directory
	tempDir, err := os.MkdirTemp("", "bundle_analysis_*")
	if err != nil {
		return err
	}
	defer func() { _ = os.RemoveAll(tempDir) }()

	err = extractZip(bundlePath, tempDir)
	if err != nil {
		return err
	}

	// Create bundle analysis
	analysis := &bundleAnalysis{
		BundlePath: bundlePath,
		Question:   question,
		Environment: make(map[string]string),
	}

	// Parse basic components
	analysis.parseEnvironment(tempDir)
	analysis.parseStatement(tempDir)
	analysis.parsePlans(tempDir)
	analysis.parseSchema(tempDir)
	analysis.generateEnhancedHypotheses()
	
	presentAnalysis(analysis, binaryPath)

	return nil
}

func extractZip(src, dest string) error {
	r, err := zip.OpenReader(src)
	if err != nil {
		return err
	}
	defer func() { _ = r.Close() }()

	for _, f := range r.File {
		path := filepath.Join(dest, f.Name)
		
		if f.FileInfo().IsDir() {
			_ = os.MkdirAll(path, f.FileInfo().Mode())
			continue
		}

		fileReader, err := f.Open()
		if err != nil {
			return err
		}

		targetFile, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, f.FileInfo().Mode())
		if err != nil {
			fileReader.Close()
			return err
		}

		_, err = io.Copy(targetFile, fileReader)
		fileReader.Close()
		targetFile.Close()

		if err != nil {
			return err
		}
	}

	return nil
}

func (analysis *bundleAnalysis) parseEnvironment(bundleDir string) {
	envPath := filepath.Join(bundleDir, "env.sql")
	content, err := os.ReadFile(envPath)
	if err != nil {
		return
	}

	lines := strings.Split(string(content), "\n")
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if strings.HasPrefix(line, "-- Version:") {
			analysis.Environment["version"] = strings.TrimPrefix(line, "-- Version:")
		} else if strings.HasPrefix(line, "-- User:") {
			analysis.Environment["user"] = strings.TrimPrefix(line, "-- User:")
		}
	}
}

func (analysis *bundleAnalysis) parseStatement(bundleDir string) {
	stmtPath := filepath.Join(bundleDir, "statement.sql")
	content, err := os.ReadFile(stmtPath)
	if err != nil {
		return
	}
	analysis.Statement = string(content)
}

func (analysis *bundleAnalysis) parsePlans(bundleDir string) {
	plans := []string{}
	
	planFiles := []string{"plan.txt", "opt.txt", "opt-v.txt", "opt-vv.txt"}
	for _, fileName := range planFiles {
		content, err := os.ReadFile(filepath.Join(bundleDir, fileName))
		if err == nil {
			plans = append(plans, fmt.Sprintf("=== %s ===\n%s", fileName, string(content)))
		}
	}
	
	analysis.Plans = plans
}

func (analysis *bundleAnalysis) parseSchema(bundleDir string) {
	analysis.Schema = &schemaInfo{}
	
	schemaPath := filepath.Join(bundleDir, "schema.sql")
	content, err := os.ReadFile(schemaPath)
	if err != nil {
		return
	}
	
	lines := strings.Split(string(content), "\n")
	var currentTable *tableInfo
	
	for _, line := range lines {
		line = strings.TrimSpace(line)
		
		// Parse CREATE TABLE statements
		if strings.HasPrefix(strings.ToUpper(line), "CREATE TABLE") {
			re := regexp.MustCompile(`CREATE TABLE\s+(?:\w+\.)?(\w+)\s*\(`)
			matches := re.FindStringSubmatch(strings.ToUpper(line))
			if len(matches) > 1 {
				currentTable = &tableInfo{Name: strings.ToLower(matches[1])}
				analysis.Schema.Tables = append(analysis.Schema.Tables, *currentTable)
			}
		} else if currentTable != nil && strings.Contains(line, " ") && !strings.HasPrefix(line, "--") {
			// Parse column definitions (simplified)
			parts := strings.Fields(line)
			if len(parts) >= 2 && !strings.Contains(strings.ToUpper(parts[0]), "CONSTRAINT") && 
				!strings.Contains(strings.ToUpper(parts[0]), "INDEX") &&
				!strings.Contains(strings.ToUpper(parts[0]), "PRIMARY") &&
				!strings.Contains(strings.ToUpper(parts[0]), "UNIQUE") {
				columnName := strings.Trim(parts[0], ",")
				// Update the last table in the slice
				if len(analysis.Schema.Tables) > 0 {
					lastIdx := len(analysis.Schema.Tables) - 1
					analysis.Schema.Tables[lastIdx].Columns = append(analysis.Schema.Tables[lastIdx].Columns, columnName)
				}
			}
		}
		
		// Reset current table when we hit the end of CREATE TABLE
		if strings.Contains(line, ");") {
			currentTable = nil
		}
	}
}

func (analysis *bundleAnalysis) generateEnhancedHypotheses() {
	analysis.Hypotheses = []hypothesis{}
	
	questionLower := strings.ToLower(analysis.Question)
	planText := strings.Join(analysis.Plans, " ")
	planUpper := strings.ToUpper(planText)
	
	// Parse statement for more detailed analysis
	stmtInfo := analysis.parseStatementDetails()
	
	// Enhanced performance analysis
	if strings.Contains(questionLower, "slow") || strings.Contains(questionLower, "performance") {
		analysis.analyzePerformanceIssues(planUpper, stmtInfo)
	}
	
	// Enhanced index analysis
	if strings.Contains(questionLower, "index") || 
	   strings.Contains(planUpper, "FULL SCAN") || 
	   strings.Contains(planUpper, "TABLE SCAN") {
		analysis.analyzeIndexOpportunities(planUpper, stmtInfo)
	}
	
	// Resource analysis
	if strings.Contains(questionLower, "resource") || strings.Contains(questionLower, "memory") || strings.Contains(questionLower, "cpu") {
		analysis.analyzeResourceUsage(planUpper, stmtInfo)
	}
	
	// Query structure analysis
	analysis.analyzeQueryStructure(stmtInfo)
}

func (analysis *bundleAnalysis) parseStatementDetails() *statementInfo {
	stmt := &statementInfo{SQL: analysis.Statement}
	
	lines := strings.Split(analysis.Statement, "\n")
	var cleanSQL strings.Builder
	
	// Remove comments and build clean SQL
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if !strings.HasPrefix(line, "--") && line != "" {
			cleanSQL.WriteString(line + " ")
		}
	}
	
	sqlText := strings.TrimSpace(cleanSQL.String())
	sqlUpper := strings.ToUpper(sqlText)
	
	// Determine statement type
	if strings.HasPrefix(sqlUpper, "SELECT") {
		stmt.Type = "SELECT"
	} else if strings.HasPrefix(sqlUpper, "INSERT") {
		stmt.Type = "INSERT"
	} else if strings.HasPrefix(sqlUpper, "UPDATE") {
		stmt.Type = "UPDATE"
	} else if strings.HasPrefix(sqlUpper, "DELETE") {
		stmt.Type = "DELETE"
	}
	
	// Extract WHERE clause
	whereIdx := strings.Index(sqlUpper, "WHERE")
	if whereIdx != -1 {
		orderIdx := strings.Index(sqlUpper[whereIdx:], "ORDER BY")
		limitIdx := strings.Index(sqlUpper[whereIdx:], "LIMIT")
		groupIdx := strings.Index(sqlUpper[whereIdx:], "GROUP BY")
		
		endIdx := len(sqlText)
		for _, idx := range []int{orderIdx, limitIdx, groupIdx} {
			if idx != -1 && idx < endIdx-whereIdx {
				endIdx = whereIdx + idx
			}
		}
		stmt.WhereClause = strings.TrimSpace(sqlText[whereIdx:endIdx])
	}
	
	// Extract ORDER BY clause
	orderIdx := strings.Index(sqlUpper, "ORDER BY")
	if orderIdx != -1 {
		limitIdx := strings.Index(sqlUpper[orderIdx:], "LIMIT")
		endIdx := len(sqlText)
		if limitIdx != -1 {
			endIdx = orderIdx + limitIdx
		}
		stmt.OrderBy = strings.TrimSpace(sqlText[orderIdx:endIdx])
	}
	
	// Extract table references
	re := regexp.MustCompile(`(?i)\b(?:FROM|JOIN|UPDATE|INTO)\s+(\w+)(?:\s+(?:AS\s+)?(\w+))?`)
	matches := re.FindAllStringSubmatch(sqlText, -1)
	for _, match := range matches {
		if len(match) > 1 {
			stmt.Tables = append(stmt.Tables, match[1])
		}
	}
	
	return stmt
}

func (analysis *bundleAnalysis) analyzePerformanceIssues(planUpper string, stmt *statementInfo) {
	if strings.Contains(planUpper, "FULL SCAN") || strings.Contains(planUpper, "TABLE SCAN") {
		h := hypothesis{
			Category:    "Performance",
			Title:       "Table Scan Performance Impact",
			Description: "Query uses full table scans which may cause slow performance",
			Evidence:    []string{"Full or table scans found in execution plan"},
			Confidence:  0.8,
			Actions: []string{
				"Add indexes on filtered columns",
				"Review WHERE clause conditions",
				"Update table statistics",
			},
		}
		
		// Generate specific index suggestions
		if stmt.WhereClause != "" {
			suggestions := analysis.generateIndexSuggestions(stmt.WhereClause, stmt.Tables)
			h.SQLSolutions = suggestions
		}
		
		analysis.Hypotheses = append(analysis.Hypotheses, h)
	}
	
	if strings.Contains(planUpper, "HASH JOIN") {
		h := hypothesis{
			Category:    "Performance", 
			Title:       "Hash Join Optimization",
			Description: "Hash joins detected - may be inefficient for large datasets",
			Evidence:    []string{"Hash joins in execution plan"},
			Confidence:  0.6,
			Actions: []string{
				"Review join predicates",
				"Consider adding indexes on join columns",
				"Analyze table sizes and statistics",
			},
			SQLSolutions: analysis.generateJoinOptimizations(stmt),
		}
		analysis.Hypotheses = append(analysis.Hypotheses, h)
	}
}

func (analysis *bundleAnalysis) analyzeIndexOpportunities(planUpper string, stmt *statementInfo) {
	if strings.Contains(planUpper, "FULL SCAN") || strings.Contains(planUpper, "TABLE SCAN") {
		h := hypothesis{
			Category:    "Indexing",
			Title:       "Missing Index Opportunities",
			Description: "Table scans indicate missing indexes for optimal performance",
			Evidence:    []string{"Full table scans detected"},
			Confidence:  0.8,
			Actions: []string{
				"Create indexes on filtered columns",
				"Add composite indexes for multi-column filters",
				"Consider covering indexes",
			},
		}
		
		// Generate specific CREATE INDEX statements
		if stmt.WhereClause != "" {
			h.SQLSolutions = analysis.generateIndexSuggestions(stmt.WhereClause, stmt.Tables)
		}
		if stmt.OrderBy != "" {
			h.SQLSolutions = append(h.SQLSolutions, analysis.generateOrderByIndexes(stmt.OrderBy, stmt.Tables)...)
		}
		
		analysis.Hypotheses = append(analysis.Hypotheses, h)
	}
}

func (analysis *bundleAnalysis) analyzeResourceUsage(planUpper string, stmt *statementInfo) {
	if strings.Contains(planUpper, "SORT") {
		h := hypothesis{
			Category:    "Resources",
			Title:       "Sort Operation Optimization",
			Description: "Sorting operations consume memory and CPU resources",
			Evidence:    []string{"Sort operations detected"},
			Confidence:  0.7,
			Actions: []string{
				"Create indexes to eliminate sorting",
				"Review ORDER BY necessity",
				"Consider query rewriting",
			},
		}
		
		if stmt.OrderBy != "" {
			h.SQLSolutions = analysis.generateOrderByIndexes(stmt.OrderBy, stmt.Tables)
		}
		
		analysis.Hypotheses = append(analysis.Hypotheses, h)
	}
}

func (analysis *bundleAnalysis) analyzeQueryStructure(stmt *statementInfo) {
	if len(analysis.Statement) > 1000 || len(stmt.Tables) > 5 {
		h := hypothesis{
			Category:    "Query Structure",
			Title:       "Query Complexity Optimization",
			Description: "Complex query structure may benefit from optimization",
			Evidence:    []string{
				fmt.Sprintf("Statement length: %d characters", len(analysis.Statement)),
				fmt.Sprintf("Tables involved: %d", len(stmt.Tables)),
			},
			Confidence:  0.6,
			Actions: []string{
				"Consider breaking into smaller queries",
				"Review join necessity",
				"Analyze subquery performance",
			},
			SQLSolutions: analysis.generateStructureOptimizations(stmt),
		}
		analysis.Hypotheses = append(analysis.Hypotheses, h)
	}
}

func (analysis *bundleAnalysis) generateIndexSuggestions(whereClause string, tables []string) []string {
	var suggestions []string
	
	// Extract column references from WHERE clause
	// This is a simplified parser - could be made more sophisticated
	re := regexp.MustCompile(`(\w+)\s*(?:=|>|<|>=|<=|!=|LIKE|IN|BETWEEN)`)
	matches := re.FindAllStringSubmatch(whereClause, -1)
	
	columnsByTable := make(map[string][]string)
	
	for _, match := range matches {
		if len(match) > 1 {
			column := match[1]
			// Try to match column to table
			for _, table := range tables {
				if analysis.Schema != nil {
					for _, tableInfo := range analysis.Schema.Tables {
						if strings.EqualFold(tableInfo.Name, table) {
							for _, col := range tableInfo.Columns {
								if strings.EqualFold(col, column) {
									columnsByTable[table] = append(columnsByTable[table], column)
								}
							}
						}
					}
				}
			}
			// If no specific table match, suggest for first table
			if len(columnsByTable) == 0 && len(tables) > 0 {
				columnsByTable[tables[0]] = append(columnsByTable[tables[0]], column)
			}
		}
	}
	
	// Generate CREATE INDEX statements
	for table, columns := range columnsByTable {
		if len(columns) == 1 {
			suggestions = append(suggestions, 
				fmt.Sprintf("CREATE INDEX idx_%s_%s ON %s (%s);", 
					table, columns[0], table, columns[0]))
		} else if len(columns) > 1 {
			// Remove duplicates
			uniqueCols := removeDuplicateStrings(columns)
			suggestions = append(suggestions,
				fmt.Sprintf("CREATE INDEX idx_%s_%s ON %s (%s);",
					table, strings.Join(uniqueCols, "_"), table, strings.Join(uniqueCols, ", ")))
		}
	}
	
	return suggestions
}

func (analysis *bundleAnalysis) generateOrderByIndexes(orderBy string, tables []string) []string {
	var suggestions []string
	
	// Extract columns from ORDER BY clause
	re := regexp.MustCompile(`ORDER BY\s+(.*?)(?:$|LIMIT|OFFSET)`)
	matches := re.FindStringSubmatch(strings.ToUpper(orderBy))
	if len(matches) > 1 {
		columns := strings.Split(matches[1], ",")
		var cleanColumns []string
		for _, col := range columns {
			col = strings.TrimSpace(col)
			col = strings.Replace(col, " ASC", "", -1)
			col = strings.Replace(col, " DESC", "", -1)
			cleanColumns = append(cleanColumns, col)
		}
		
		if len(tables) > 0 && len(cleanColumns) > 0 {
			suggestions = append(suggestions,
				fmt.Sprintf("CREATE INDEX idx_%s_ordered ON %s (%s);",
					tables[0], tables[0], strings.Join(cleanColumns, ", ")))
		}
	}
	
	return suggestions
}

func (analysis *bundleAnalysis) generateJoinOptimizations(stmt *statementInfo) []string {
	var suggestions []string
	
	if len(stmt.Tables) >= 2 {
		// Suggest indexes on join columns (simplified heuristic)
		for i, table := range stmt.Tables {
			if i > 0 {
				suggestions = append(suggestions,
					fmt.Sprintf("-- Consider adding index on join columns for table %s", table))
				suggestions = append(suggestions,
					fmt.Sprintf("CREATE INDEX idx_%s_join ON %s (id); -- Adjust column name as needed", table, table))
			}
		}
	}
	
	return suggestions
}

func (analysis *bundleAnalysis) generateStructureOptimizations(stmt *statementInfo) []string {
	var suggestions []string
	
	if len(stmt.Tables) > 3 {
		suggestions = append(suggestions, "-- Consider breaking this query into multiple steps:")
		for i, table := range stmt.Tables {
			if i < 2 {
				continue // Skip first two tables for the example
			}
			suggestions = append(suggestions,
				fmt.Sprintf("-- Step %d: Create temporary result from %s", i-1, table))
		}
	}
	
	return suggestions
}

func removeDuplicateStrings(slice []string) []string {
	keys := make(map[string]bool)
	var result []string
	for _, item := range slice {
		if !keys[item] {
			keys[item] = true
			result = append(result, item)
		}
	}
	return result
}

func presentAnalysis(analysis *bundleAnalysis, binaryPath string) {
	fmt.Printf("=== CockroachDB Statement Bundle Analysis ===\n\n")
	fmt.Printf("Bundle: %s\n", analysis.BundlePath)
	fmt.Printf("Question: %s\n\n", analysis.Question)
	
	// Present environment summary
	if len(analysis.Environment) > 0 {
		fmt.Printf("=== Environment ===\n")
		for key, value := range analysis.Environment {
			fmt.Printf("%s: %s\n", strings.Title(key), strings.TrimSpace(value))
		}
		fmt.Printf("\n")
	}
	
	// Present statement summary
	if analysis.Statement != "" {
		fmt.Printf("=== Statement Analysis ===\n")
		lines := strings.Split(strings.TrimSpace(analysis.Statement), "\n")
		if len(lines) > 0 {
			firstLine := strings.TrimSpace(lines[0])
			if len(firstLine) > 100 {
				firstLine = firstLine[:100] + "..."
			}
			fmt.Printf("First line: %s\n", firstLine)
		}
		fmt.Printf("Total lines: %d\n", len(lines))
		fmt.Printf("\n")
	}
	
	// Present plan summary
	if len(analysis.Plans) > 0 {
		fmt.Printf("=== Plan Analysis ===\n")
		fmt.Printf("Available plans: %d\n", len(analysis.Plans))
		fmt.Printf("\n")
	}
	
	// Present hypotheses
	fmt.Printf("=== Generated Hypotheses ===\n")
	if len(analysis.Hypotheses) == 0 {
		fmt.Printf("No specific hypotheses generated. This might indicate:\n")
		fmt.Printf("- The query is already well-optimized\n")
		fmt.Printf("- The question requires manual analysis of the bundle\n")
		fmt.Printf("- More specific information is needed\n\n")
	} else {
		// Sort by confidence
		sort.Slice(analysis.Hypotheses, func(i, j int) bool {
			return analysis.Hypotheses[i].Confidence > analysis.Hypotheses[j].Confidence
		})
		
		for i, h := range analysis.Hypotheses {
			fmt.Printf("%d. [%s] %s (Confidence: %.0f%%)\n", i+1, h.Category, h.Title, h.Confidence*100)
			fmt.Printf("   Description: %s\n", h.Description)
			if len(h.Evidence) > 0 {
				fmt.Printf("   Evidence:\n")
				for _, ev := range h.Evidence {
					fmt.Printf("     - %s\n", ev)
				}
			}
			if len(h.Actions) > 0 {
				fmt.Printf("   Recommended Actions:\n")
				for _, action := range h.Actions {
					fmt.Printf("     - %s\n", action)
				}
			}
			if len(h.SQLSolutions) > 0 {
				fmt.Printf("   SQL Solutions:\n")
				for _, sql := range h.SQLSolutions {
					if strings.HasPrefix(sql, "--") {
						fmt.Printf("     %s\n", sql)
					} else {
						fmt.Printf("     %s\n", sql)
					}
				}
			}
			fmt.Printf("\n")
		}
	}
	
	// Present recreation command
	fmt.Printf("=== Bundle Recreation ===\n")
	fmt.Printf("To recreate this bundle environment, run:\n")
	fmt.Printf("  %s debug statement-bundle recreate <extracted_bundle_dir>\n\n", binaryPath)
	
	// Present next steps
	fmt.Printf("=== Next Steps ===\n")
	fmt.Printf("1. Review the hypotheses above in order of confidence\n")
	fmt.Printf("2. Use the recreation command to test solutions\n")
	fmt.Printf("3. Compare execution plans before and after optimizations\n")
	fmt.Printf("4. Monitor performance improvements in production\n")
}
