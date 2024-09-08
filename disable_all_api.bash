# List all services that are enabled on the project
enabled_services=$(gcloud services list --enabled --format="value(config.name)")

# Loop through the list of enabled services
for service in $enabled_services; do
    if [ "$service" != "translate.googleapis.com" ]; then
        echo "Disabling $service..."
        gcloud services disable $service --quiet
    else
        echo "Skipping Cloud Translation API..."
    fi
done

echo "All services except Cloud Translation API have been disabled."