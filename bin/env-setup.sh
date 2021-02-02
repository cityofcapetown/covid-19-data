#!/usr/bin/env bash

echo "=> Setting up City Service Analysis Environment <="
echo ""

DB_UTILS_LOCATION=https://ds2.capetown.gov.za/db-utils
DB_UTILS_PKG=db_utils-0.3.7-py2.py3-none-any.whl
TMP=/tmp/

nslookup ds2.capetown.gov.za > /dev/null
if [[ $? -eq 0 ]]; then
  echo "Installing db-utils"
  cd "$TMP"
  wget --quiet "$DB_UTILS_LOCATION"/"$DB_UTILS_PKG"
  pip3 install "$DB_UTILS_PKG" &> /dev/null
  cd -
else
  echo "Can not resolve ds2.capetown.gov.za"
fi

exit_code=$?

cd "$CITY_SERVICE_DIR"

echo "=> Done setting up City Service Analysis Environment <="
echo ""

exit $exit_code
