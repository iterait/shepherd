Shepherd API
============

.. raw:: html

    <div id="swagger-ui"></div>

    <script src="_static/swagger-ui-bundle.js"></script>
    <script src="_static/swagger-ui-standalone-preset.js"></script>

    <script>
    window.onload = function() {
      const ui = SwaggerUIBundle({
        url: "https://github.com/iterait/shepherd/releases/download/v0.4.1/swagger.yml",
        dom_id: '#swagger-ui',
        supportedSubmitMethods: [], // disable "Try it" buttons
        requestInterceptor: function() {
            this.url = "https://cors-anywhere.herokuapp.com" + '/' + this.url // bypass GitHub CORS
            return this;
        },
        presets: [
          SwaggerUIBundle.presets.apis,
          SwaggerUIStandalonePreset
        ]
      })

      window.ui = ui
    }
    </script>
