#listing folders and file within folders

#list all the folders from drive
def list_folders(service):
    """Lists all folders in Google Drive."""
    results = service.files().list(
        q="mimeType='application/vnd.google-apps.folder'",
        fields="nextPageToken, files(id, name)").execute()
    items = results.get('files', [])
    
    if not items:
        print('No folders found.')
    else:
        print('Folders:')
        for item in items:
            print(f"{item['name']} ({item['id']})")


#get all the file and folder from the parent folder(given id)
def list_files_in_folder(folder_id, service):
    """List all files in a specified Google Drive folder."""
    try:
        results = service.files().list(
            q=f"'{folder_id}' in parents",
            fields="nextPageToken, files(id, name)").execute()
        items = results.get('files', [])
        
        if not items:
            print('No files found in the folder.')
        else:
            print('Files in the folder:')
            for item in items:
                print(f"{item['name']} ({item['id']})")
    except Exception as e:
        print(f'An error occurred: {e}')