var AppStatistic = new Vue({
    el: '#statistics',
    delimiters: ['[[', ']]'],
    data() {
        return {
            showArchiveData: false,
            currentCanvas: null,
            oldCanvas: null,
            loading: true,
        }
    },
    mounted() {
        this.loading = false
    },
    methods: {
        changeCurrentCanvas(event) {
            this.oldCanvas = this.currentCanvas
            this.currentCanvas = event.target
            if (this.oldCanvas === this.currentCanvas) {
                if (this.currentCanvas.getAttribute('class') === 'btn btn-success') {
                    this.currentCanvas.setAttribute('class', 'btn btn-secondary')
                    return
                } 
                if (this.currentCanvas.getAttribute('class') === 'btn btn-secondary') {
                    this.currentCanvas.setAttribute('class', 'btn btn-success')
                    return
                } 
            }
            this.currentCanvas.setAttribute('class', 'btn btn-success')
            if (this.oldCanvas) {                              
                this.oldCanvas.setAttribute('class', 'btn btn-secondary')                
            }   
            this.currentCanvas.focus()                
        },
        async get_link(event) {
            const response = await fetch('/api/v1/get-zoom-link/',{
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json'
                },
                body: JSON.stringify({
                    dataLink: event.target.dataset.link
                })
            })
            link = await response.json()
            if (link['data-link']) {
                event.target.setAttribute('href', link['data-link'])
                event.target.setAttribute('target', '_blank')
                event.target.setAttribute('class', 'btn btn-success')
                event.target.innerHTML = 'Перейти'
            } else {
                event.target.innerHTML = 'отсутствует'
                event.target.setAttribute('class', 'btn btn-secondary disabled')
            }
        },
        expandCollapse(event) {
            if (event.target.getAttribute('aria-expanded') === 'true') {
                event.target.innerHTML = 'Свернуть'
            } 
            if (event.target.getAttribute('aria-expanded') === 'false') {
                event.target.innerHTML = 'Развернуть'
            }
        }
    }, 
  })