#!/bin/bash
# The sandbox user may only create/use throw-away databases named sbx_*.
# It has no access to any other schema and the source database is a
# different server it holds no credentials for.
set -euo pipefail
mysql -uroot -p"${MYSQL_ROOT_PASSWORD}" <<SQL
GRANT ALL PRIVILEGES ON \`sbx\\_%\`.* TO '${MYSQL_USER}'@'%';
FLUSH PRIVILEGES;
SQL
