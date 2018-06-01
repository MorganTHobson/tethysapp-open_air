from tethys_sdk.base import TethysAppBase, url_map_maker
from tethys_sdk.app_settings import PersistentStoreDatabaseSetting

class OpenAir(TethysAppBase):
    """
    Tethys app class for Baltimore Open Air.
    """

    name = 'Baltimore Open Air'
    index = 'open_air:home'
    icon = 'open_air/images/icon.gif'
    package = 'open_air'
    root_url = 'open-air'
    color = '#42585b'
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
            UrlMap(
                name='sensors',
                url='open-air/sensors',
                controller='open_air.controllers.list_sensors'
            ),
            UrlMap(
                name='update_sensor',
                url='open-air/sensors/update',
                controller='open_air.controllers.update_sensor'
            ),
            UrlMap(
                name='graphs',
                url='open-air/graphs/{temperature_graph_id}',
                controller='open_air.controllers.temperature_graph'
            ),
            UrlMap(
                name='graphs_ajax',
                url='open-air/graphs/{sensor_id}/ajax',
                controller='open_air.controllers.graphs_ajax'
            ),
            UrlMap(
                name='user_guide',
                url='open-air/user-guide',
                controller='open_air.controllers.user_guide'
            )
        )

        return url_maps

    def persistent_store_settings(self):
        """
        Define Persistent Store Settings.
        """
        ps_settings = (
            PersistentStoreDatabaseSetting(
                name='sensor_db',
                description='sensor database',
                initializer='open_air.model.init_sensor_db',
                required=True
            ),
        )

        return ps_settings
