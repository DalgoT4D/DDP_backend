#!/bin/bash

# Path to your .env file
ENV_FILE_PATH="./.env"



# Read the .env file line by line
while IFS='=' read -r key value
do
  # Remove potential whitespace around the key
  key=$(echo "$key" | xargs)

  # Use only non-empty lines that do not start with '#'
  if [[ ! -z "$key" && ! $key == \#* ]]; then
    # Process the value if needed (e.g., remove inline comments)
    value=${value%%\#*}       # Del everything after '#'
    value=$(echo "$value" | xargs) # Remove potential whitespace

    # Define the secret name based on the key
    SECRET_NAME="${key}"

    # Check if the secret already exists
    if gcloud secrets describe "$SECRET_NAME" &>/dev/null; then
      # Add a new version if the secret exists
      echo "Adding new version to secret $SECRET_NAME"
      echo -n "$value" | gcloud secrets versions add "$SECRET_NAME" --data-file=-
    else
      # Create the secret if it does not exist
      echo "Creating secret $SECRET_NAME"
      echo -n "$value" | gcloud secrets create "$SECRET_NAME" --replication-policy="automatic" --data-file=-
    fi
  fi
done < "$ENV_FILE_PATH"