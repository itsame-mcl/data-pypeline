import geojson
import matplotlib.cm as cm
import matplotlib.colors as clrs
import matplotlib.pyplot as plt
from descartes import PolygonPatch
from Pipeline import Pipelineable


class ExportMapFRMetro(Pipelineable):
    def __init__(self, path, type_geo, var_geocode, var_tomap, title=None, color_scale='viridis',
                 display_labels=True, display_scale=True):
        self.__path = path
        if type_geo == "dep" or type_geo == "departement":
            self.__json_path = "data/departements.geojson"
        elif type_geo == "reg" or type_geo == "region":
            self.__json_path = "data/regions.geojson"
        else:
            raise ValueError
        self.__var_geocode = var_geocode
        self.__var_tomap = var_tomap
        self.__title = title
        self.__color_scale = color_scale
        self.__display_labels = display_labels
        self.__display_scale = display_scale

    def apply(self, df):
        data = dict(zip(df[self.__var_geocode], df[self.__var_tomap]))
        fig = plt.figure()
        ax = fig.gca()
        if self.__title:
            fig.suptitle(self.__title)
        cols = plt.get_cmap(self.__color_scale)
        norm = clrs.Normalize()
        norm.autoscale(list(data.values()))
        if self.__display_scale:
            fig.colorbar(cm.ScalarMappable(norm=norm, cmap=cols), ax=ax)
        with open(self.__json_path) as json_file:
            geo_data = geojson.load(json_file)
        for geo in geo_data['features']:
            if geo['properties']['code'] in list(data.keys()):
                val = data[geo['properties']['code']]
                color = clrs.to_hex(cols(norm(val)))
            else:
                color = '#808080'
            geo_area = PolygonPatch(geo['geometry'], fc=color, alpha=0.5, zorder=1)
            if self.__display_labels:
                shape = geo_area.get_window_extent()
                cx = shape.xmin + shape.size[0]/2
                cy = shape.ymin + shape.size[1]/2
                ax.text(cx, cy, geo['properties']['code'], fontsize=5, ha='center')
            ax.add_patch(geo_area)
        ax.axis([-6, 10, 40, 52])
        ax.axes.get_xaxis().set_visible(False)
        ax.axes.get_yaxis().set_visible(False)
        plt.savefig(self.__path)
        return df