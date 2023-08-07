const App = {
    delimiters: ['[[', ']]'],
    data() {
        return {
            zoomOn: true,
            placeholder: 'Фамилия и Имя менеджера',
            files: [],
            isLoading: false,      
        }
    },
    methods: {        
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
            try {
                const directory_handle = await showDirectoryPicker();
                const user_files =  await this.get_user_files()
                const x = new Date();
                const currentTimeZoneOffsetInHours = x.getTimezoneOffset() / 60;

                await this.read_directory(directory_handle);                    

                let form = new FormData();
                this.files.forEach((file, index) => {
                    form.append(file.directory, file.file)
                });                    
                form.append('s3_files', user_files['cloudfiles'])
                form.append('currentTimeZoneOffsetInHours', currentTimeZoneOffsetInHours)

                const response = await fetch('/api/v1/zoom-upload',{
                    method: 'POST',
                    // headers: {'Content-Type': 'application/json'},
                    body: form,
                })
                
                this.isLoading = false;
                
                res = await response.json()
                console.log(res)
                return {'status': 'ok'}
                
            } catch (error) {
                console.warn(error)
                this.isLoading = false;
            }              
        },
        async get_user_files() {
                const response = await fetch('/api/v1/get-user-files',{
                method: 'POST',
            })
            return await response.json()
        }
    }
}

Vue.createApp(App).mount('#app')