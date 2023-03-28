#!/bin/bash -efx

if [ "$1" == '--no-op' ]
then
  echo='echo'
else
  echo=
fi

PYSPARKLER_VERSION=$(poetry version --short --dry-run);

# Check if version is development release
if [[ ${PYSPARKLER_VERSION} =~ dev.* ]]; then
  # Append epoch time to dev version
  PYSPARKLER_VERSION="${PYSPARKLER_VERSION}$(date +'%s')"
fi

${echo} poetry version "${PYSPARKLER_VERSION}"
