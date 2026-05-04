openapi-generator generate \
  -i activityinfo_openapi.json \
  -g python \
  -o ./activityinfo \
  --additional-properties=library=asyncio,httpx=true,packageName=client
find ./activityinfo/client -type f -name "*.py" \
  -exec sed -i '' 's/from client/from activityinfo.client/g' {} +
find ./activityinfo/client -type f -name "*.py" \
  -exec sed -i '' 's/import client/import activityinfo.client/g' {} +
find ./activityinfo/client -type f -name "*.py" \
  -exec sed -i '' 's/getattr(client.models/getattr(activityinfo.client.models/g' {} +

# Fix duplicate retries in configuration.py
sed -i '' '/retries: Optional\[int\] = None,/d' ./activityinfo/client/configuration.py
# Fix double docstring for retries
sed -i '' '/:param retries: int - Retry configuration./d' ./activityinfo/client/configuration.py