import numpy as np
import shapefile as shp
import matplotlib.pyplot as plt
import matplotlib.colors as clrs
import matplotlib.cm as cm
import os
from jellyfish import levenshtein_distance


class CartoPlot:
    '''Class providing an easy way to plot geographic data.

    Based on département and région shapes of France.

    Parameters
    ----------
    departement_shp_path : str
        path to the shapefile (.shp) with the départements
    regions_shp_path : str
        path to the shapefile (.shp) with régions

    Attributes
    ----------
    __sf_dep : shapefile.Reader
        shapefile data for départements
    __sf_reg : shapefile.Reader
        shapefile data for régions
    __cmap : matplotlib.colors.Colormap
        colormap to use to fill the shapes with

    Examples
    --------
    >>> cp = CartoPlot()

    >>> d = {}
    >>> d['Betagni'] = 1
    >>> fig = cp.plot_reg_map(data=d)
    >>> fig.show()
    >>> fig.savefig('regions.test.jpg')

    >>> d = {}
    >>> for i in range(1, 96):
    >>>     d[str(i)] = i
    >>> del(d['69'])
    >>> d['69D'] = 69
    >>> d['69M'] = 69
    >>> d['2A'] = 20
    >>> d['2B'] = 20.5
    >>> fig = cp.plot_dep_map(data=d, x_lim=(-6, 10), y_lim=(41, 52))
    >>> fig.show()
    >>> fig.savefig('departements.test.jpg')
    '''

    def __init__(self,
                 departement_shp_path=os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                                   'departements-20180101.shp'),
                 regions_shp_path=os.path.join(os.path.dirname(os.path.abspath(__file__)), 'regions-20180101.shp'),
                 colormap='viridis'):
        '''Create the object and gather the data on département and régions.

        Parameters
        ----------
        departement_shp_path : str
            path to the shapefile (.shp) with the départements
        regions_shp_path : str
            path to the shapefile (.shp) with régions
        colormap : str or matplotlib.colors.Colormap = 'viridis'
            argument to get a colormap using matplotlib.cm.get_cmap()

        Examples
        --------
        >>> cp = CartoPlot()
        '''
        self.__sf_dep = shp.Reader(departement_shp_path)
        self.__sf_reg = shp.Reader(regions_shp_path)
        self.__cmap = plt.get_cmap(colormap)

    def plot_reg_map(self, data={}, show_name=True, d_lim=(None, None), x_lim=None, y_lim=None, figsize=(11, 9)):
        '''Plot France's map with Régions and optional data.

        Possibility to set the axes limits to restrict to a subarea.

        Parameters
        ----------
        data : dictionnary = {}
            the dictionnary with the data to convert to a list. Keys correspond to régions
        show_name : bool = True
            whether to show the name of the internal identifier of the régions
        d_lim : tuple = (None, None)
            data limits for the colormap. If None, max and/or min are computed and used
        x_lim : tuple = None
            spatial limits (longitude min, longitude max)
        y_lim : tuple = None
            spatial limits (latitude min, latitude max)
        figsize : tuple = (11, 9)
            size of the figure

        Returns
        -------
        matplotlib.figure.Figure
            figure objet from matplotlib with the map of the régions and the data

        Examples
        --------
        >>> cp = CartoPlot()
        >>> d = {}
        >>> d['Betagni'] = 1
        >>> fig = cp.plot_reg_map(data=d)
        >>> fig.show()
        '''
        nrm = CartoPlot.__normalizer(data, d_lim)

        return CartoPlot.__plot_map_base(
            self.__sf_reg,
            data=CartoPlot.__data_list(
                self.__sf_reg,
                1,
                data,
                nrm=nrm),
            cmap=self.__cmap,
            nrm=nrm,
            x_lim=x_lim,
            y_lim=y_lim,
            figsize=figsize,
            label_record_idx=1 if show_name else 0)

    def plot_dep_map(self, data={}, show_name=False, d_lim=(None, None), x_lim=None, y_lim=None, figsize=(11, 9)):
        '''Plot France's map with Départments and optional data.

        Possibility to set the axes limits to restrict to a subarea.

        Parameters
        ----------
        data : dictionnary
            the dictionnary with the data to convert to a list. Keys correspond to départements
            identifiers (such as '35' for 'Ille et Vilaine', or '2B' for 'Haute Corse')
        show_name : bool = True
            whether to show the name of the internal identifier of the déparements
        d_lim : tuple = (None, None)
            data limits for the colormap. If None, max and/or min are computed and used
        x_lim : tuple = None
            spatial limits (longitude min, longitude max)
        y_lim : tuple = None
            spatial limits (latitude min, latitude max)
        figsize : tuple = (11, 9)
            size of the figure

        Returns
        -------
        matplotlib.figure.Figure
            figure objet from matplotlib with the map of the départements and the data

        Examples
        --------
        >>> cp = CartoPlot()
        >>> d = {}
        >>> d['35'] = 1
        >>> fig = cp.plot_dep_map(data=d)
        >>> fig.show()
        '''
        # Preprocess départements to pad them with zeros when needed
        data = dict(zip(['0{}'.format(x) if len(x) < 2 else x for x in data.keys()], data.values()))

        nrm = CartoPlot.__normalizer(data, d_lim)

        return CartoPlot.__plot_map_base(
            self.__sf_dep,
            data=CartoPlot.__data_list(
                self.__sf_dep,
                0,
                data,
                levenshtein_threshold=3 if show_name else 0,
                nrm=nrm),
            cmap=self.__cmap,
            nrm=nrm,
            x_lim=x_lim,
            y_lim=y_lim,
            figsize=figsize,
            label_record_idx=1 if show_name else 0)

    @staticmethod
    def __normalizer(data, d_lim=(None, None)):
        '''Create an object to normalize data

        Parameters
        ----------
        data : dict
            the dictionnary with the data to convert to a list. Keys correspond to shapes
        d_lim : tuple = (None, None)
            data limits for the colormap. If None, max and/or min are computed and used

        Returns
        -------
        matplotlib.colors.Normalize
            the matplotlib object to normalize data
        '''
        # Initialize the data normalization object
        d_min, d_max = d_lim
        if d_lim[0] is None:
            d_min = min(data.values())
        if d_lim[1] is None:
            d_max = max(data.values())
        nrm = clrs.Normalize(vmin=d_min, vmax=d_max, clip=True)

        return nrm

    @staticmethod
    def __data_list(sf, record_idx, data, levenshtein_threshold=3, nrm=clrs.Normalize(0, 1, True)):
        '''Convert a data dictionnary to a data list

        This static method can be modified and adapted to the choice of data type for the
        variable data.

        Parameters
        ----------
        sf : shapefile.Reader
            the variable holding the shapefile data
        record_idx : int
            the index in the shape record pointing to the record name to match the data keys
        data : dict
            the dictionnary with the data to convert to a list. Keys correspond to shapes
        levenshtein_threshold : int = 3
            threshold to consider the string approximately equal in the Levenshtein distance
        nrm : matplotlib.colors.Normalize = matplotlib.colors.Normalize(0, 1, True)
            the matplotlib object to normalize data

        Returns
        -------
        list
            data list with indices corresponding to shapes in the shapefile, to be used in
            __plot_map_base()
        '''
        # Initialise the output to the right size
        data_list = [None] * len(sf.shapeRecords())

        # Loop through the shapes
        for shape_idx, shape in enumerate(sf.shapeRecords()):
            # Get the record name we want the keys of the dictionnary to match to
            rec_idx = shape.record[record_idx]

            # Two possibilities: exact or approximate match
            if rec_idx in data:
                data_list[shape_idx] = nrm(data[rec_idx])
            elif levenshtein_threshold > 0:
                # Using Levenshtein distance to allow for some discrepancy
                for data_key, data_value in data.items():
                    if levenshtein_distance(data_key, rec_idx) <= levenshtein_threshold:
                        data_list[shape_idx] = nrm(data_value)
                        break

        return data_list

    @staticmethod
    def __plot_map_base(sf, data=[], nrm=clrs.Normalize(0, 1, True), cmap=plt.get_cmap('viridis'), x_lim=None,
                        y_lim=None, figsize=(11, 9), label_record_idx=0):
        '''Base function to plot shapes to form a map, and data to shade those shapes.

        The variable data could be changed to fit whatever type is needed in the project.

        Parameters
        ----------
        data : list = []
            data list with indices corresponding to shapes in the shapefile, to be used in
            __plot_map_base()
        nrm : matplotlib.colors.Normalize = matplotlib.colors.Normalize(0, 1, True)
            the matplotlib object to normalize data
        cmap : matplotlib.colors.Colormap = matplotlib.pyplot.get_cmap('viridis')
            colormap to use to fill the shapes with
        x_lim : tuple = None
            spatial limits (longitude min, longitude max)
        y_lim : tuple = None
            spatial limits (latitude min, latitude max)
        figsize : tuple = (11, 9)
            size of the figure
        label_record_idx : int = 0
            the index of the label to plot in the shapes' records

        Returns
        -------
        matplotlib.figure.Figure
            figure objet from matplotlib with the map of the départements and the data
        '''
        # Figure preparation
        fig = plt.figure(figsize=figsize)
        fig.clf()

        # Axes prepration, with no frame and no ticks
        ax = fig.add_subplot(1, 1, 1, frame_on=False)
        ax.tick_params(left=False,
                       bottom=False,
                       labelleft=False,
                       labelbottom=False)
        fig.colorbar(cm.ScalarMappable(norm=nrm, cmap=cmap), ax=ax)

        # Go through all shapes
        for shape_idx, shape in enumerate(sf.shapeRecords()):
            # Go through all parts (islands and such)
            start_part = 0
            for start_next_part in list(shape.shape.parts[1:]) + [len(shape.shape.points)]:
                x = [i[0] for i in shape.shape.points[start_part:start_next_part]]
                y = [i[1] for i in shape.shape.points[start_part:start_next_part]]
                ax.plot(x, y, 'k')
                if len(data) > 0 and data[shape_idx] is not None:
                    ax.fill(x, y, clrs.to_hex(cmap(data[shape_idx])))
                start_part = start_next_part

            # Find the center point of the shape
            x = [i[0] for i in shape.shape.points]
            y = [i[1] for i in shape.shape.points]
            x0 = np.mean([min(x), max(x)])
            y0 = np.mean([min(y), max(y)])

            # Add the shape's label
            ax.text(x0, y0, shape.record[label_record_idx], fontsize=10, horizontalalignment='center')

        # The the axes limits
        if (x_lim is not None) and (y_lim is not None):
            ax.set_xlim(x_lim)
            ax.set_ylim(y_lim)

        return fig
