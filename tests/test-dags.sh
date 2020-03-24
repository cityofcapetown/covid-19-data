#/usr/bin/env bash
set -e

for dag_file in $(ls dags/); do
  echo "Testing "$dag_file"..."
  python3 dags/"$dag_file"
done