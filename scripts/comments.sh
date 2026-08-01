#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 ReifyDB
#
# strip: rewrite .rs files, stripping comments outside #[cfg(test)] sections
#        (keeping TODO/FIXME/XXX/HACK/BUG/SAFETY/SOUND markers).
# check: report comments misplaced around tests - above #[test]/#[tokio::test]/
#        chaos_test!(...) instead of inside the test body, or wedged between an
#        attribute and the fn signature.
#
# Exit code (check): 0 if no violations, 1 if violations found.
set -euo pipefail

usage() {
    echo "Usage:" >&2
    echo "  $0 strip [--dry-run] [--path DIR] [file...]" >&2
    echo "  $0 check" >&2
    exit 1
}

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
    if ($t eq "" || $t =~ m{SPDX-License-Identifier|Copyright}) {
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
                    # Keep the whole block: a marker's continuation lines are not
                    # themselves markers and would otherwise be stripped, truncating
                    # the comment mid-sentence.
                    $out .= $block . ($block_end < $n ? "\n" : "");
                    $i = $block_end < $n ? $block_end + 1 : $block_end;
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

cmd_strip() {
    local DRY_RUN=false
    local PATH_ARG="crates"
    local CHANGED=0
    local EXPLICIT_FILES=()

    while [[ $# -gt 0 ]]; do
        case $1 in
            --dry-run) DRY_RUN=true; shift ;;
            --path)    PATH_ARG="${2:?}"; shift 2 ;;
            *)         EXPLICIT_FILES+=("$1"); shift ;;
        esac
    done

    local files
    if [[ ${#EXPLICIT_FILES[@]} -gt 0 ]]; then
        mapfile -t files < <(printf '%s\n' "${EXPLICIT_FILES[@]}")
    else
        mapfile -t files < <(find "$PATH_ARG" -name "*.rs" -not -path "*/tests/*" | sort)
    fi

    local TMPFILE
    TMPFILE=$(mktemp)
    trap "rm -f '$TMPFILE'" EXIT INT TERM

    for file in "${files[@]}"; do
        [[ "$file" =~ /tests/ ]] && continue
        case "$(basename "$file")" in lib.rs|mod.rs) continue ;; esac

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
}

# Classify each line of a .rs file (COMMENT/ATTR_TEST/ATTR_OTHER/CHAOS/BLANK/CODE),
# masking string literals and block comments first, then print "LINE:KIND" for
# every misplaced-comment violation found:
#   above_test      - a comment sits directly above #[test]/#[tokio::test]
#                      (walking up through any stacked attributes first)
#   between_attr_fn - a comment sits between the attribute and the fn line
#   above_chaos     - a comment sits directly above a chaos_test!(...) call
check_file() {
    awk '
    BEGIN {
        in_block_comment = 0
        block_re = "/[*][^*]*([*][^/][^*]*)*[*]/"
    }
    {
        line = $0

        if (in_block_comment) {
            if (match(line, /\*\//)) {
                line = substr(line, RSTART + RLENGTH)
                in_block_comment = 0
            } else {
                cls[NR] = "COMMENT"
                next
            }
        }

        while (match(line, block_re)) {
            line = substr(line, 1, RSTART - 1) substr(line, RSTART + RLENGTH)
        }

        if (match(line, /\/\*/)) {
            pre = substr(line, 1, RSTART - 1)
            trimmed_pre = pre
            gsub(/^[ \t]+|[ \t]+$/, "", trimmed_pre)
            line = pre
            in_block_comment = 1
            if (trimmed_pre == "") { cls[NR] = "COMMENT"; next }
        }

        gsub(/"[^"]*"/, "", line)

        if (match(line, /\/\//)) {
            prefix = substr(line, 1, RSTART - 1)
            trimmed = prefix
            gsub(/^[ \t]+|[ \t]+$/, "", trimmed)
            cls[NR] = (trimmed == "") ? "COMMENT" : "CODE"
            next
        }

        trimmed = line
        gsub(/^[ \t]+|[ \t]+$/, "", trimmed)

        if (trimmed == "") { cls[NR] = "BLANK"; next }
        if (trimmed ~ /^#\[[ \t]*(tokio::)?test[ \t]*\]$/) { cls[NR] = "ATTR_TEST"; next }
        if (trimmed ~ /^#\[.*\]$/) { cls[NR] = "ATTR_OTHER"; next }
        if (trimmed ~ /^chaos_test![ \t]*\(/) { cls[NR] = "CHAOS"; next }
        cls[NR] = "CODE"
    }
    END {
        total = NR
        for (i = 1; i <= total; i++) {
            if (cls[i] == "ATTR_TEST") {
                top = i
                while (top - 1 >= 1 && cls[top-1] == "ATTR_OTHER") top--
                if (top - 1 >= 1 && cls[top-1] == "COMMENT") {
                    start = top - 1
                    while (start - 1 >= 1 && cls[start-1] == "COMMENT") start--
                    print start ":above_test"
                }
                k = i + 1
                while (k <= total && (cls[k] == "ATTR_OTHER" || cls[k] == "ATTR_TEST")) k++
                if (k <= total && cls[k] == "COMMENT") {
                    print k ":between_attr_fn"
                }
            }
            if (cls[i] == "CHAOS") {
                top = i
                while (top - 1 >= 1 && cls[top-1] == "ATTR_OTHER") top--
                if (top - 1 >= 1 && cls[top-1] == "COMMENT") {
                    start = top - 1
                    while (start - 1 >= 1 && cls[start-1] == "COMMENT") start--
                    print start ":above_chaos"
                }
            }
        }
    }
    ' "$1"
}

cmd_check() {
    local repo_root
    repo_root=$(git rev-parse --show-toplevel 2>/dev/null || pwd)

    echo "Checking for misplaced test comments in /crates/, /pkg/, /bin/..."
    echo ""

    local rs_files
    rs_files=$(find "$repo_root/crates" "$repo_root/pkg" "$repo_root/bin" -name "*.rs" \
        -not -path "*/vendor/*" \
        -not -path "*/generated/*" \
        -not -path "*/target/*" 2>/dev/null || true)

    if [ -z "$rs_files" ]; then
        echo "No Rust files found"
        exit 0
    fi

    local violations_found=false
    local violation_count=0

    while IFS= read -r file; do
        local result
        result=$(check_file "$file")

        if [ -n "$result" ]; then
            while IFS=: read -r line_num kind; do
                local content
                content=$(sed -n "${line_num}p" "$file")

                if [ "$violations_found" = false ]; then
                    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
                    echo "❌ Test comment placement violations detected!"
                    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
                    echo ""
                    violations_found=true
                fi

                local reason
                case "$kind" in
                    above_test)      reason="sits above #[test]/#[tokio::test] instead of inside the test body" ;;
                    between_attr_fn) reason="sits between the attribute and fn instead of inside the test body" ;;
                    above_chaos)     reason="sits above chaos_test!(...) instead of inside its closure body" ;;
                esac

                local rel_path="${file#$repo_root/}"
                echo "  📄 $rel_path:$line_num"
                echo "     $content"
                echo "     ($reason)"
                echo ""
                violation_count=$((violation_count + 1))
            done <<< "$result"
        fi
    done <<< "$rs_files"

    if [ "$violations_found" = true ]; then
        echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
        echo "Found $violation_count violation(s)"
        echo ""
        echo "A test's explanatory comment belongs on the first line(s) inside its"
        echo "body, never above #[test]/#[tokio::test]/chaos_test!(...), and never"
        echo "between an attribute and the fn signature."
        echo ""
        echo "Example:"
        echo "  ❌ // A replica commit adopts the primary's version verbatim..."
        echo "     #[test]"
        echo "     fn test_replica_versions() { ... }"
        echo ""
        echo "  ✅ #[test]"
        echo "     fn test_replica_versions() {"
        echo "         // A replica commit adopts the primary's version verbatim..."
        echo "         ..."
        echo "     }"
        echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
        exit 1
    else
        echo "✅ No test comment placement violations found!"
        exit 0
    fi
}

[ $# -ge 1 ] || usage
cmd="$1"
shift

case "$cmd" in
    strip) cmd_strip "$@" ;;
    check) cmd_check "$@" ;;
    *)     usage ;;
esac
