package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"

	"github.com/rohanjq/alerts/internal/definitionfile"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, os.Args[1:], os.Stdout, os.Stderr); err != nil {
		fmt.Fprintln(os.Stderr, "alertdefs:", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	if len(args) == 0 {
		usage(stderr)
		return errors.New("expected export or import command")
	}
	switch args[0] {
	case "export":
		return runExport(ctx, args[1:], stdout, stderr)
	case "import":
		return runImport(ctx, args[1:], stdout, stderr)
	case "help", "-h", "--help":
		usage(stdout)
		return nil
	default:
		usage(stderr)
		return fmt.Errorf("unknown command %q", args[0])
	}
}

func runExport(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	flags := flag.NewFlagSet("export", flag.ContinueOnError)
	flags.SetOutput(stderr)
	apiURL := flags.String("api-url", env("ALERTS_API_URL", "http://127.0.0.1:8100"), "Alerts control API URL")
	apiToken := flags.String("api-token", os.Getenv("ALERTS_API_TOKEN"), "Alerts control API bearer token")
	output := flags.String("out", "-", "output file, or - for stdout")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 0 {
		return errors.New("export does not accept positional arguments")
	}
	manifest, err := (definitionfile.Client{BaseURL: *apiURL, Token: *apiToken}).Export(ctx)
	if err != nil {
		return err
	}
	if *output == "-" {
		return definitionfile.Encode(stdout, manifest)
	}
	if err := writeManifest(*output, manifest); err != nil {
		return err
	}
	fmt.Fprintf(stderr, "exported %d definitions to %s\n", len(manifest.Definitions), *output)
	return nil
}

func runImport(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	flags := flag.NewFlagSet("import", flag.ContinueOnError)
	flags.SetOutput(stderr)
	apiURL := flags.String("api-url", env("ALERTS_API_URL", "http://127.0.0.1:8100"), "Alerts control API URL")
	apiToken := flags.String("api-token", os.Getenv("ALERTS_API_TOKEN"), "Alerts control API bearer token")
	input := flags.String("file", "-", "input file, or - for stdin")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 0 {
		return errors.New("import does not accept positional arguments")
	}
	manifest, err := readManifest(*input, os.Stdin)
	if err != nil {
		return err
	}
	result, err := (definitionfile.Client{BaseURL: *apiURL, Token: *apiToken}).Import(ctx, manifest)
	if err != nil {
		return err
	}
	fmt.Fprintf(stdout, "loaded %d definitions: %d created, %d already present, %d enabled states updated\n", len(manifest.Definitions), result.Created, result.Existing, result.StateUpdated)
	for _, warning := range result.NameMismatches {
		fmt.Fprintln(stderr, "warning:", warning)
	}
	return nil
}

func readManifest(path string, stdin io.Reader) (definitionfile.Manifest, error) {
	if path == "-" {
		return definitionfile.Decode(stdin)
	}
	file, err := os.Open(path)
	if err != nil {
		return definitionfile.Manifest{}, fmt.Errorf("open %s: %w", path, err)
	}
	defer file.Close()
	return definitionfile.Decode(file)
}

func writeManifest(path string, manifest definitionfile.Manifest) (err error) {
	directory := filepath.Dir(path)
	temporary, err := os.CreateTemp(directory, "."+filepath.Base(path)+".tmp-*")
	if err != nil {
		return fmt.Errorf("create temporary export beside %s: %w", path, err)
	}
	temporaryPath := temporary.Name()
	defer func() {
		_ = temporary.Close()
		if err != nil {
			_ = os.Remove(temporaryPath)
		}
	}()
	if err = temporary.Chmod(0o644); err != nil {
		return fmt.Errorf("set permissions on temporary export: %w", err)
	}
	if err = definitionfile.Encode(temporary, manifest); err != nil {
		return err
	}
	if err = temporary.Sync(); err != nil {
		return fmt.Errorf("sync temporary export: %w", err)
	}
	if err = temporary.Close(); err != nil {
		return fmt.Errorf("close temporary export: %w", err)
	}
	if err = os.Rename(temporaryPath, path); err != nil {
		return fmt.Errorf("replace %s: %w", path, err)
	}
	return nil
}

func usage(writer io.Writer) {
	fmt.Fprintln(writer, "usage: alertdefs export [-api-url URL] [-api-token TOKEN] [-out FILE]")
	fmt.Fprintln(writer, "       alertdefs import [-api-url URL] [-api-token TOKEN] [-file FILE]")
}

func env(name, fallback string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}
	return fallback
}
