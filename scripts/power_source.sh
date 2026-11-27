#!/usr/bin/env sh
# Prints the host's power source: `ac`, `battery` or `unknown`. The timed gates
# give no verdict for a run seen on battery (GH-86). `unknown` is a host this
# cannot answer for, such as a server that exposes no power supply.
if command -v pmset >/dev/null 2>&1; then
  case "$(pmset -g batt 2>/dev/null | sed -n 1p)" in
    *"'AC Power'"*) echo ac ;;
    *"'Battery Power'"*) echo battery ;;
    *) echo unknown ;;
  esac
  exit 0
fi

source=unknown
for supply in /sys/class/power_supply/*; do
  [ -r "$supply/type" ] || continue
  # A peripheral's battery says nothing about what powers the host.
  [ "$(cat "$supply/scope" 2>/dev/null)" = Device ] && continue
  case "$(cat "$supply/type")" in
    Battery)
      if [ "$(cat "$supply/status" 2>/dev/null)" = Discharging ]; then
        echo battery
        exit 0
      fi
      ;;
    Mains)
      [ "$(cat "$supply/online" 2>/dev/null)" = 1 ] && source=ac
      ;;
  esac
done
echo "$source"
