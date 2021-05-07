import geojson
import matplotlib.cm as cm
import matplotlib.colors as clrs
import matplotlib.pyplot as plt
from descartes import PolygonPatch
from Pipeline import Pipelineable


class ExportMap(Pipelineable):
    def __init__(self, path, type, var_geocode, var_tomap, title=None, color_scale='viridis'):
        self.__path = path
        if type == "dep" or type == "departement":
            self.__json_path = "../data/departements.geojson"
        elif type == "reg" or type == "region":
            self.__json_path = "../data/regions.geojson"
        else:
            raise ValueError
        self.__var_geocode = var_geocode
        self.__var_tomap = var_tomap
        self.__title = title
        self.__color_scale = color_scale

    def apply(self, df):
        data = dict(zip(df[self.__var_geocode], df[self.__var_tomap]))
        fig = plt.figure()
        ax = fig.gca()
        if self.__title:
            fig.suptitle(self.__title)
        cols = plt.get_cmap(self.__color_scale)
        norm = clrs.Normalize()
        norm.autoscale(list(data.values()))
        fig.colorbar(cm.ScalarMappable(norm=norm, cmap=cols), ax=ax)
        with open(self.__json_path) as json_file:
            geo_data = geojson.load(json_file)
        for geo in geo_data['features']:
            if geo['properties']['code'] in list(data.keys()):
                val = data[geo['properties']['code']]
                color = clrs.to_hex(cols(norm(val)))
            else:
                color = '#808080'
            ax.add_patch(PolygonPatch(geo['geometry'], fc=color, alpha=0.5, zorder=2))
        ax.axis([-6, 10, 40, 52])
        ax.axes.get_xaxis().set_visible(False)
        ax.axes.get_yaxis().set_visible(False)
        plt.savefig(self.__path)
        return df
