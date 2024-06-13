#!/bin/sh

# Ścieżka do pliku wskaźnikowego
FLAG_FILE=/app/.first_run_completed

# Sprawdzenie zmiennej środowiskowej DELAY_SECONDS
if [ ! -f "$FLAG_FILE" ]; then
  echo "Pierwsze uruchomienie, brak opóźnienia."
  touch "$FLAG_FILE"
else
  if [ -n "$DELAY_SECONDS" ]; then
    echo "Opóźnienie uruchomienia o $DELAY_SECONDS sekund..."
    sleep $DELAY_SECONDS
  fi
fi

# Uruchomienie głównej aplikacji
exec python consumer.py
