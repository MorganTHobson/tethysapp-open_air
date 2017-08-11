from tethys_sdk.base import TethysAppBase, url_map_maker


class OpenAir(TethysAppBase):
    """
    Tethys app class for Baltimore Open Air.
    """

    name = 'Baltimore Open Air'
    index = 'open_air:home'
    icon = 'open_air/images/icon.gif'
    package = 'open_air'
    root_url = 'open-air'
    color = '#b9d0f3'
    description = 'Front end sensor monitoring'
    tags = 'air quality'
    enable_feedback = False
    feedback_emails = []

    def url_maps(self):
        """
        Add controllers
        """
        UrlMap = url_map_maker(self.root_url)

        url_maps = (
            UrlMap(
                name='home',
                url='open-air',
                controller='open_air.controllers.home'
            ),
        )

        return url_maps
