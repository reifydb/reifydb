#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2025 ReifyDB
set -euo pipefail

DRY_RUN=false
PATH_ARG="crates"
CHANGED=0
EXPLICIT_FILES=()

while [[ $# -gt 0 ]]; do
    case $1 in
        --dry-run) DRY_RUN=true; shift ;;
        --path)    PATH_ARG="${2:?}"; shift 2 ;;
        *)         EXPLICIT_FILES+=("$1"); shift ;;
    esac
done

process_file() {
    perl - "$1" << 'ENDPERL'
use strict;
use warnings;

my @MARKERS = qw(TODO FIXME XXX HACK BUG SAFETY SOUND);

sub is_marker {
    my $s = shift;
    $s =~ s{^[/* !\t]+}{};
    for my $m (@MARKERS) { return 1 if index($s, $m) == 0 }
    0
}

open(my $fh, "<:raw", $ARGV[0]) or die "open $ARGV[0]: $!\n";
my $content = do { local $/; <$fh> };
close $fh;

my $n   = length $content;
my $i   = 0;
my $out = "";

# Header: preserve all leading blank/comment lines before first code line.
while ($i < $n) {
    my $sol = $i;
    $i++ while $i < $n && substr($content, $i, 1) ne "\n";
    my $line = substr($content, $sol, $i - $sol);
    (my $t = $line) =~ s/^\s+|\s+$//g;
    if ($t eq "" || $t =~ m{^//}) {
        $out .= $line . ($i < $n ? "\n" : "");
        $i++ if $i < $n;
    } else {
        $i = $sol;
        last;
    }
}

# Body: strip comments outside test sections.
# states: 0=NORMAL 1=IN_STRING 2=IN_RAW_STRING 3=IN_LINE_COMMENT
my ($state, $raw_h, $in_test, $ptest, $tdepth) = (0, 0, 0, 0, 0);

while ($i < $n) {
    my $c = substr($content, $i, 1);

    # Pass #[cfg(test)] sections through unchanged.
    if ($in_test || $ptest) {
        $out .= $c;
        if ($state == 0) {
            if ($c eq '"') {
                $state = 1;
            } elsif ($c eq '{') {
                if ($ptest) { $ptest = 0; $in_test = 1; $tdepth = 1 }
                else        { $tdepth++ }
            } elsif ($c eq '}' && $in_test) {
                $tdepth--;
                $in_test = 0 if $tdepth == 0;
            }
        } elsif ($state == 1) {
            if ($c eq "\\" && $i+1 < $n) { $out .= substr($content, ++$i, 1) }
            elsif ($c eq '"')            { $state = 0 }
        }
        $i++;
        next;
    }

    if ($state == 0) {

        # String literal.
        if ($c eq '"') { $out .= $c; $state = 1; $i++; next }

        # Raw string: r#*"..."#*
        if ($c eq 'r') {
            my ($j, $h) = ($i+1, 0);
            $h++, $j++ while $j < $n && substr($content, $j, 1) eq '#';
            if ($j < $n && substr($content, $j, 1) eq '"') {
                $out .= substr($content, $i, $j-$i+1);
                ($state, $raw_h, $i) = (2, $h, $j+1);
                next;
            }
        }

        # / potentially starting a comment.
        if ($c eq '/' && $i+1 < $n) {
            my $nc = substr($content, $i+1, 1);

            if ($nc eq '/') {
                # Find extent of contiguous // block: this line plus any
                # following lines whose first non-whitespace chars are //.
                my $block_end = $i;
                $block_end++ while $block_end < $n && substr($content, $block_end, 1) ne "\n";
                while ($block_end < $n) {
                    my $j = $block_end + 1;
                    $j++ while $j < $n && (substr($content, $j, 1) eq ' ' || substr($content, $j, 1) eq "\t");
                    if ($j + 1 < $n && substr($content, $j, 2) eq '//') {
                        $block_end = $j;
                        $block_end++ while $block_end < $n && substr($content, $block_end, 1) ne "\n";
                    } else {
                        last;
                    }
                }
                my $block = substr($content, $i, $block_end - $i);
                if (index($block, '# Safety') >= 0) {
                    $out .= $block . ($block_end < $n ? "\n" : "");
                    $i = $block_end < $n ? $block_end + 1 : $block_end;
                    next;
                }

                my $end = $i;
                $end++ while $end < $n && substr($content, $end, 1) ne "\n";
                my $cm = substr($content, $i, $end-$i);
                if (is_marker($cm)) {
                    $out =~ s/[ \t]+\z//;
                    $out .= $cm . ($end < $n ? "\n" : "");
                    $i = $end < $n ? $end+1 : $end;
                } else {
                    $out =~ s/[ \t]+\z//;
                    $state = 3; $i += 2;
                }
                next;
            }

            if ($nc eq '*') {
                my ($end, $d) = ($i+2, 1);
                while ($end < $n && $d > 0) {
                    if    (substr($content, $end, 2) eq '/*') { $d++; $end += 2 }
                    elsif (substr($content, $end, 2) eq '*/')  { $d--; $end += 2 }
                    else                                       { $end++ }
                }
                my $cm = substr($content, $i, $end-$i);
                if (is_marker(substr($cm, 2)) || index($cm, '# Safety') >= 0) {
                    $out .= $cm;
                } else {
                    $out .= "\n" x ($cm =~ tr/\n//);
                }
                $i = $end; next;
            }
        }

        # #[cfg(test)] line.
        if ($c eq '#' && $i+1 < $n && substr($content, $i+1, 1) eq '[') {
            my $end = $i;
            $end++ while $end < $n && substr($content, $end, 1) ne "\n";
            my $line = substr($content, $i, $end-$i);
            (my $t = $line) =~ s/^\s+|\s+$//g;
            if ($t eq '#[cfg(test)]') {
                $ptest = 1;
                $out .= $line . ($end < $n ? "\n" : "");
                $i = $end < $n ? $end+1 : $end;
                next;
            }
        }

        $out .= $c; $i++;

    } elsif ($state == 1) {
        $out .= $c;
        if    ($c eq "\\" && $i+1 < $n) { $out .= substr($content, ++$i, 1) }
        elsif ($c eq '"')               { $state = 0 }
        $i++;

    } elsif ($state == 2) {
        $out .= $c;
        if ($c eq '"') {
            my ($j, $h) = ($i+1, 0);
            $h++, $j++ while $j < $n && substr($content, $j, 1) eq '#' && $h < $raw_h;
            if ($h == $raw_h) {
                $out .= substr($content, $i+1, $j-$i-1);
                $state = 0; $i = $j; next;
            }
        }
        $i++;

    } elsif ($state == 3) {
        if ($c eq "\n") { $out .= $c; $state = 0 }
        $i++;
    }
}

binmode STDOUT, ":raw";
print $out;
ENDPERL
}

if [[ ${#EXPLICIT_FILES[@]} -gt 0 ]]; then
    mapfile -t files < <(printf '%s\n' "${EXPLICIT_FILES[@]}")
else
    mapfile -t files < <(find "$PATH_ARG" -name "*.rs" -not -path "*/tests/*" | sort)
fi

TMPFILE=$(mktemp)
trap 'rm -f "$TMPFILE"' EXIT INT TERM

for file in "${files[@]}"; do
    [[ "$file" =~ /tests/ ]] && continue

    process_file "$file" > "$TMPFILE"

    if ! cmp -s "$file" "$TMPFILE"; then
        if $DRY_RUN; then
            diff -u "$file" "$TMPFILE" || true
        else
            cp "$TMPFILE" "$file"
            echo "  $file" >&2
            CHANGED=$((CHANGED + 1))
        fi
    fi
done

echo "" >&2
if $DRY_RUN; then
    echo "Would change: $CHANGED files" >&2
else
    echo "Changed: $CHANGED files" >&2
fi
