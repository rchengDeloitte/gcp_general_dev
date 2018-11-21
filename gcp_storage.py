import pandas as pd
import os

class gcp_storage:
    ## can make this path as a global veriable
    GOOGLE_APPLICATION_CREDENTIALS_STORAGE = 'autho_k/storage/gcp-generalcode-dev-rubycau-6d75b5337213.json'
    
    def __init__(self,bucket_name):
        """
        Explicitly use service account credentials by specifying the private key file.
        """
        self.client = storage.Client.from_service_account_json('credentials/LIVERPOOL DEMO-e778084b3d0f.json')
        self.bucket = self.client.get_bucket(bucket_name)
        
        return        
        
    def change_bucket(self,bucket_name):
        """
        change bucket within the same project if needed
        """
        self.bucket = self.client.get_bucket(bucket_name)
        
        return    
    
    def read_to_str(self,path_filename):
        """
        read file as string
        """       
        blob = self.bucket.blob(path_filename)
        
        return blob.download_as_string()
    
    
    def read_csv(self,path_filename):
        """
        read csv file to a pandas dataframe
        """
        iofile = self.read_to_str(path_filename)
        df = pd.read_csv(io.BytesIO(iofile),low_memory=False)
        
        return df
    

    
    def upload_file_string(self, file_stream, path_filename, content_type):
        """
        Uploads a string to a given Cloud Storage bucket and returns the public url
        to the new object.
        """
        #fow_logger.info('filename ' + str(filename) + " b " + str(bucket_name))
        blob = bucket.blob(path_filename)

        #fow_logger.info("uploading file " + str(filename) + " into " + str(bucket_name))
        blob.upload_from_string(
            file_stream,
            content_type=content_type)
#         fow_logger.info("Uploaded input files into " + str(bucket_name))
        
        return blob
    
    def upload_file(self, file, path_filename, content_type):
        """
        Uploads a file object.
        example: open('file.py') as file: upload_file
        
        """
        #fow_logger.info('filename ' + str(filename) + " b " + str(bucket_name))
        blob = bucket.blob(path_filename)

        #fow_logger.info("uploading file " + str(filename) + " into " + str(bucket_name))
        blob.upload_from_file(
            file,
            content_type=content_type)
#         fow_logger.info("Uploaded input files into " + str(bucket_name))
        
        return blob
    
    def upload_filename(self, filename, path_filename):
        """
        Uploads a filename (path where the file is in logal env)
        """
        #fow_logger.info('filename ' + str(filename) + " b " + str(bucket_name))
        blob = bucket.blob(path_filename)

        #fow_logger.info("uploading file " + str(filename) + " into " + str(bucket_name))
        blob.upload_from_filename('temp_folder/'+filename)
#         fow_logger.info("Uploaded input files into " + str(bucket_name))
        
        return blob
    
    
    def upload_df_csv(self,df,filename,path):
        """
        upload pandas dataframe to storage as a CSV file
        """
        df.to_csv('temp_dir/'+filename)# create file 
        path_filename = path+'/'+filename# for blob
        
        self.upload_filename('temp_dir/'+filename, path_filename)
        os.remove('temp_dir/'+filename)# delete file after upload
        
        return 
        
        
      
    def upload_model_joblib(self,model,filename,path):
       
        joblib.dump(model, 'temp_dir/'+filename)# create file 
        path_filename = path+'/'+filename

        self.upload_filename('temp_dir/'+filename, path_filename)
        os.remove('temp_dir/'+filename)# delete file after upload
        
        return 
    
    
    
    # Export the model to a file
# model = 'model.joblib'
# joblib.dump(pipeline, model)

# # Upload the model to GCS
# bucket = storage.Client().bucket(BUCKET_NAME)
# blob = bucket.blob('{}/{}'.format(
#     datetime.datetime.now().strftime('census_%Y%m%d_%H%M%S'),
#     model))
# blob.upload_from_filename(model)
    
     

        
        
        
        