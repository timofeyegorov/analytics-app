const App = {
    delimiters: ['[[', ']]'],
    data() {
        return {
            zoomOn: true,
            placeholder: 'Фамилия и Имя менеджера',
            files: [],
            isLoading: false, 
            selectedManager: '',
            data_last_uploaded_zoom: null                      
        }
    },
    computed: {
        last_uploaded_zoom() {
            this.get_last_uploded_zoom()
            return this.data_last_uploaded_zoom
        }
    },
    methods: {  
        async get_last_uploded_zoom() {
            const response = await fetch(`/api/v1/get-last-uploaded-zoom/${this.selectedManager}`,{
                method: 'GET',
            })
            datetime_zoom = await response.json()
            if (datetime_zoom['datetime_zoom']) {
                this.data_last_uploaded_zoom = `Последняя дата загрузки ${datetime_zoom['datetime_zoom']} мск.`
            }
            else {
                this.data_last_uploaded_zoom = null
            }
            
        },

        async read_directory(directory_handle, path_prefix) {
            for await(let handle of directory_handle.values()) {
                // console.log(handle)
                if (handle.kind === "directory") {
                    // console.group(`Catalog ${handle.name}`)
                    await this.read_directory(handle, path_prefix ? `${path_prefix}/${handle.name}` : handle.name);
                }
                if (handle.kind === "file") {
                    // console.log(handle);
                    await handle.getFile().then(value => {
                        if (value) {
                            this.files.push({
                                directory: path_prefix || "",
                                file: value
                            });
                        }                    
                    });
                }
            }
        },
        async upload_zoom() { 
            
            this.isLoading = true;
            const user = await this.get_user_name()
            if (user) {
                try {
                    const directory_handle = await showDirectoryPicker();                
                    await this.read_directory(directory_handle);                              
                    const cloudfiles =  await this.get_cloudfiles() 
                    const datetime_checked_files = await this.get_datetime_checked_files()
                    const zoom_timeframes = await this.get_zoom_timeframes()
                    const x = new Date();
                    const currentTimeZoneOffsetInHours = x.getTimezoneOffset() / 60;
                    const credential = await this.get_credential()
                    updated = false
                    const unique_date_set = []
                    const temp_files = []
                    // console.log(zoom_timeframes)                     
                    
                    if (datetime_checked_files) {
                        for (const data of datetime_checked_files) {
                            if (!unique_date_set.includes(data.directory)) {
                                unique_date_set.push(data.directory)
                            }
                        }
                        unique_date_set.sort(this.date_compare).slice(0, 20)                                                               
                        
                        for (const data of datetime_checked_files) {
                            if (unique_date_set.includes(data.directory)) {
                                date_in_path = data.directory.substring(0, 19)
                                date_in_path = date_in_path.replace(' ', 'T').replace('.', ':').replace('.', ':')
                            
                                for (const zt of zoom_timeframes['zoom_timeframes']) {
                                    const zt_base = zt[0]
                                    const zt_start = zt[1]
                                    const zt_end = zt[2] 
                                    
                                    if (this.check_date_include(
                                        date_in_path, zt_start, zt_end, currentTimeZoneOffsetInHours
                                        )) {
                                            files_to_upload = `${user.username}/${this.get_zoom_datetime(zt_base)}/${data.directory}/${data.file.name}`;                                
                                            if (!cloudfiles.cloudfiles.includes(files_to_upload)) {
                                                const filename = '${filename}'                                                                                                          
                                                const resp = await this.upload_file(
                                                    `temp/${user.username}/${this.get_zoom_datetime(zt_base)}/${data.directory}/${filename}`,
                                                    credential.forms3.xAmzCredential,
                                                    credential.forms3.xAmzAlgorithm,
                                                    credential.forms3.xAmzDate,
                                                    credential.forms3.policy,
                                                    credential.forms3.XAmzSignature,
                                                    data.file,
                                                    files_to_upload                                        
                                                )
                                                updated = true
                                                if (resp) {
                                                    temp_path = `${user.username}/${this.get_zoom_datetime(zt_base)}`
                                                    if (!temp_files.includes(temp_path)) {
                                                        temp_files.push(temp_path)
                                                    }                                                    
                                                }
                                            } else {
                                                console.warn('Current Files Uploaded')
                                            }                                                                
                                        }                       
                                }
                                if (updated) {
                                    await this.update_upload_date()
                                }
                            }                          
                        }
                    } else {
                        console.error('File To Upload Not Found')
                    }               
                    
                    this.isLoading = false;                    
                    if (temp_files.length !== 0) {
                        console.log(temp_files)
                        const res = await this.copy_tempfiles(temp_files)
                        console.log(await res.json())
                    }                    
                } catch (error) {
                    console.warn(error)
                    this.isLoading = false;
                } 
            } else {
                console.warn('invalid credentials (failed)')
                this.isLoading = false; 
            }                        
        },
        date_compare(a, b){
            a = a.substring(0, 19).replace(' ', 'T').replace('.', ':').replace('.', ':')
            b = b.substring(0, 19).replace(' ', 'T').replace('.', ':').replace('.', ':')
            zt_a = new Date(a)
            zt_b = new Date(b)
            if (zt_a < zt_b) {
                return 1;
              }
              if (zt_a > zt_b) {
                return -1;
              }              
              return 0;
        },
        async copy_tempfiles(tempfiles) {
            const requestsOptions = {
                method: "POST",
                headers: {
                    "Content-Type": "application/json"
                },
                body: JSON.stringify({
                    tempfiles: tempfiles
                })
            };
            const response = await fetch('/api/v1/complete-zoom-upload', requestsOptions)
            return response
        },
        
        async update_upload_date() {
            const response = await fetch('/api/v1/update-upload-date', {
                method: 'POST',
            })
        },

        async get_credential() {
            const response = await fetch('/api/v1/zoom-upload',{
                method: 'POST',
                // headers: {'Content-Type': 'application/json'},
            })
            return await response.json()
        },

        async get_cloudfiles() {
                const response = await fetch('/api/v1/get-user-files',{
                method: 'POST',
            })
            return await response.json()
        },

        async get_zoom_timeframes() {
            const response = await fetch('/api/v1/user-zoom-timeframes',{
                method: 'POST',
            })
            return await response.json()
        },

        async get_user_name() {
            const response = await fetch('/api/v1/get-user-name',{
                method: 'POST',
            })
            return await response.json()
        },

        async check_date_format(date_string) {
            const regex = /^\d{4}-\d{2}-\d{2} \d{2}\.\d{2}\.\d{2}$/;
            return regex.test(date_string)            
        },

        async get_datetime_checked_files() { 
            let files_to_upload = [];      
            for (let i = 0; i < this.files.length; i++) {
                const data_in_path = this.files[i].directory.substring(0, 19);
                if (await this.check_date_format(data_in_path)) {
                    files_to_upload.push(this.files[i])
                }              
            }
            return files_to_upload
        },

        get_zoom_datetime(datetime_string) {
            const dateObj = new Date(datetime_string);
            const year = dateObj.getFullYear();
            const month = (dateObj.getMonth() + 1).toString().padStart(2, '0');
            const day = dateObj.getDate().toString().padStart(2, '0');
            const hours = dateObj.getHours().toString().padStart(2, '0');
            const minutes = dateObj.getMinutes().toString().padStart(2, '0');

            const formattedDate = `${year}${month}${day}-${hours}${minutes}`;
            return formattedDate
        },
        check_date_include(zt, zt_start, zt_end, tz_offset) {
            tz_msk = 3
            base = new Date(zt)
            base.setHours(base.getHours() + tz_offset + tz_msk);
            start = new Date(zt_start)
            end = new Date(zt_end)
            return (start < base) && (base < end)
        },        
              
        async upload_file(key, xAmzCredential, xAmzAlgorithm, xAmzDate, policy, XAmzSignature, file, uploaded_file) {
            const formS3 = new FormData();
            formS3.append('key', key)
            formS3.append('X-Amz-Credential', xAmzCredential)
            formS3.append('X-Amz-Algorithm', xAmzAlgorithm)
            formS3.append('X-Amz-Date', xAmzDate)
            formS3.append('Policy', policy)
            formS3.append('X-Amz-Signature', XAmzSignature)
            formS3.append('file', file)            

            const response = await fetch('https://anu-zoom-01.s3.storage.selcloud.ru/',{
                method: 'POST',
                mode: "no-cors",
                body: formS3,
            })
            if (response) {
                console.log('uploaded: ', uploaded_file)
            }
            return response                        
        },
    }
}

Vue.createApp(App).mount('#app')