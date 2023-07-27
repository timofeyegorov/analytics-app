"use strict";


(($) => {


    let read_directory = async (directory_handle, files, path_prefix) => {
        for await(let handle of directory_handle.values()){
            if (handle.kind === "directory") {
                await read_directory(handle, files, path_prefix ? `${path_prefix}/${handle.name}` : handle.name);
            }
            if (handle.kind === "file") {
                handle.getFile().then(value => {
                    files.push({
                        directory: path_prefix || "",
                        file: value
                    });
                });
            }
        }
    }


    $(() => {

        $("button.btn-primary").bind("click", async (event) => {
            let files = [];
            event.preventDefault();
            const directory_handle = await self.showDirectoryPicker();
            await read_directory(directory_handle, files);
            let form = new FormData();
            files.forEach((file, index) => {
                form.append(`file${index}`, file.file);
                form.append(`directory${index}`, file.directory);
            });
            form.append("manager", $("#field-user").val());
            $.ajax({
                url: "/api/s3-upload",
                data: form,
                type: "POST",
                processData: false,
                contentType: false,
                success: (d,f,g) => {
                    console.log("SUCCESS!", d, f, g);
                },
                error: (d,f,g) => {
                    console.log("ERROR!", d, f, g);
                },
                complete: (d,f,g) => {
                    console.log("COMPLETE!", d, f, g);
                }
            })
        });

    });


})(jQuery);
