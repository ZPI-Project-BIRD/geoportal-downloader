package processor

import (
	"context"
	"fmt"
	"os/exec"
	"strings"
)

type Tiler interface {
	Tile(context.Context, string, string) error
}

type GDALTiler struct {
	Command string
	Zoom    string
}

func (t GDALTiler) Tile(ctx context.Context, inputPath, outputDirectory string) error {
	command := t.Command
	if command == "" {
		command = "gdal2tiles.py"
	}

	args := []string{
		"--xyz",
		"--webviewer=none",
		"--resampling=bilinear",
		"--processes=1",
		"--tiledriver=PNG",
	}
	if t.Zoom != "" {
		args = append(args, "--zoom", t.Zoom)
	}
	args = append(args, inputPath, outputDirectory)

	output, err := exec.CommandContext(ctx, command, args...).CombinedOutput()
	if err != nil {
		return fmt.Errorf("run %s: %w: %s", command, err, strings.TrimSpace(string(output)))
	}
	return nil
}
