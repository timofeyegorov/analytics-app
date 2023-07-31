const App = {
    delimiters: ['[[', ']]'],
    data() {
        return {
            placeholder: 'Фамилия и Имя менеджера',
            managerUser: '',
            files: [],      
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
            if (this.managerUser) {
                // console.log('upload_zoom')
                const directory_handle = await showDirectoryPicker();
                await this.read_directory(directory_handle);
                // console.log(this.files)

                let form = new FormData();
                this.files.forEach((file, index) => {
                    form.append(file.directory, file.file)
                });
                form.append('manager', this.managerUser)
                const response = await fetch('/api/v1/zoom-upload',{
                    method: 'POST',
                    // headers: {'Content-Type': 'application/json'},
                    body: form,
                })
                this.managerUser = ''
                console.log(response.status)
            } else {
                alert('Вам необходимо заполнить поле <менеджер>')
            }           
        },
    }
}

Vue.createApp(App).mount('#app')