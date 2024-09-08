from googleapiclient.http import MediaFileUpload, MediaIoBaseDownload
import os,io,pandas as pd


def download_file(file_id, file_name, service, local_folder):
    """Download an Excel file from Google Drive and save it locally."""
    # Define paths
    excel_path = os.path.join(local_folder, file_name)
    csv_path = os.path.join(local_folder, f"{os.path.splitext(file_name)[0]}.csv")
    
    # Download the file from Google Drive
    request = service.files().get_media(fileId=file_id)
    with io.FileIO(excel_path, 'wb') as fh:
        downloader = MediaIoBaseDownload(fh, request)
        done = False
        while not done:
            status, done = downloader.next_chunk()
            print(f"Downloading {file_name}: {int(status.progress() * 100)}%")

    # Convert the downloaded Excel file to CSV
    excel_to_csv(excel_path, csv_path)

    print(f"Downloaded {file_name} to {excel_path}")
    print(f"Converted {file_name} to {csv_path}")
    return file_name, excel_path, csv_path

def excel_to_csv(excel_path, csv_path):
    """Convert an Excel file to CSV format and save it."""
    # Ensure the directory exists
    os.makedirs(os.path.dirname(csv_path), exist_ok=True)
    
    try:
        # Read the Excel file
        df = pd.read_excel(excel_path, engine='openpyxl')
        
        # Write to CSV
        df.to_csv(csv_path, index=False, encoding='utf-8')
        
        print(f"Converted {excel_path} to {csv_path}")
    except Exception as e:
        print(f"Error converting Excel to CSV: {e}")

    
    
def download_folder(folder_id, service, local_folder):
    """Recursively download all files and folders from the given Google Drive folder."""
    if not os.path.exists(local_folder):
        os.makedirs(local_folder)
    
    try:
        results = service.files().list(
            q=f"'{folder_id}' in parents",
            fields="nextPageToken, files(id, name, mimeType)").execute()
        items = results.get('files', [])

        if not items:
            print(f'No files found in folder: {local_folder}')
            return

        for item in items:
            file_id = item['id']
            file_name = item['name']
            mime_type = item['mimeType']

            if mime_type == 'application/vnd.google-apps.folder':
                # Recursively download the contents of the folder
                new_local_folder = os.path.join(local_folder, file_name)
                print(f"Found folder: {file_name}, downloading contents...")
                download_folder(file_id, service, new_local_folder)
            else:
                # Download the file
                download_file(file_id, file_name, service, local_folder)

    except Exception as e:
        print(f'An error occurred: {e}')
