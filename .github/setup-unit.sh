if [ -n "$GOOGLE_SERVICE_ACCOUNT" ]; then
  echo $GOOGLE_SERVICE_ACCOUNT | base64 -d > src/test/resources/.gcp-service-account.json
fi
