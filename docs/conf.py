import sys
import os
import pkg_resources

sys.path.insert(0, '_base')
from conf import *

autoapi_modules = {
    'shepherd': {
        # 'override': True,
        # 'output': 'auto',
        'prune': True
    }
}

# General information about the project.
project = 'shepherd'
copyright = '2018, Iterait a.s.'
author = 'Jan Buchar, Adam Blazek, Petr Belohlavek'

# The short X.Y version.
version = '.'.join(pkg_resources.get_distribution("shepherd").version.split('.')[:2])
# The full version, including alpha/beta/rc tags.
release = pkg_resources.get_distribution("shepherd").version

html_context.update(analytics_id="UA-108491604-2")

html_theme_options.update({
    # Navigation bar title. (Default: ``project`` value)
    'navbar_title': "shepherd",

    # Tab name for entire site. (Default: "Site")
    'navbar_site_name': "Pages",

    # A list of tuples containing pages or urls to link to.
    'navbar_links': [
        ("Introduction", "tutorial"),
        ("Bare Sheep", "bare_sheep"),
        ("Docker Sheep", "docker_sheep"),
        ("Runners", "runners"),
        ("API", "api"),
        ("Package Reference", "shepherd/index"),
    ],

    # HTML navbar class (Default: "navbar") to attach to <div> element.
    # For black navbar, do "navbar navbar-inverse"
    'navbar_class': "navbar navbar-worker",
})

html_static_path += [os.path.join(os.path.dirname(__file__), '_static')]

def setup(app):
    app.add_stylesheet("highlight.css")
    app.add_stylesheet("swagger-ui.css")
