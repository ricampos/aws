#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
probmaps.py

VERSION AND LAST UPDATE:
 v1.0  06/09/2023
 v1.1  01/06/2024
 v1.2  02/24/2024
 v1.3  07/31/2024
 v2.0  09/11/2024
 v3.0  08/18/2025
 v3.1  08/22/2025

PURPOSE:
 This program generates probability maps based on ensemble forecasts of
  winds and waves. Currently it is ready to read three options of ensemble
  products: NOAA/GEFS, ECMWF, and EnvCanada/CMCE.
 It requires the forecast files to have been previously downloaded. See download_GEFSwaves.sh, 
  download_ECMWF_ENS_wind.sh, download_ECMWF_ENS_wave.sh, download_GEPS_wind.sh, and
  download_GEWPS_wave.sh
  https://github.com/NOAA-EMC/gefswaves_reforecast/tree/main/download_and_plot
 The global hazards outlook (probability map) shows the probability of 
  certain variable (wind speed or significant wave height) to have at least one value 
  above certain pre-defined level within the given time range.
 For example, selecting the week2 time range (from day 7 to day 14), it
  calculates the probabilities of encontering at least one instant above
  qlev (defined in probmaps_gefs.yaml) during this week (for each grid point).

USAGE:
 The information is passed to the script through 5 input arguments:
  1) .yaml configuration file;
  2) forecast cycle (YYYYMMDDHH) that will be used to compute the probabilities;
  3) initial day to define the time interval to calculate the statistics;
  4) final day to define the time interval to calculate the statistics;
  5) variable (select only one) to be processed: WS10 or Hs;
 The configuration probmaps_*.yaml contains the fixed information.
 This script must be run for each variable (WS10, Hs) separately.
 See the probmaps_*.yaml for more specific information and how to calibrate
  the probabilities and customize the plots.

 Example (from linux terminal command line):
  python3 probmaps_gefs.py probmaps_gefs_internal.yaml 2023060900 7 14 Hs
  nohup python3 probmaps_gefs.py probmaps_gefs_internal.yaml 2023060900 7 14 Hs >> nohup_probmaps_gefs.out 2>&1 &

OUTPUT:
 png format figures (probability maps) saved in the outpath directory informed in
 the configuration file probmaps_*.yaml

DEPENDENCIES:
 See the imports below.

AUTHOR and DATE:
 06/09/2023: Ricardo M. Campos, first version.
 01/06/2024: Ricardo M. Campos, kmz format added and variable Tp removed. The date
  has been removed from the output file name (figure) so it can be replaced automatically
  aften each new run.
 02/24/2024: Ricardo M. Campos, include 5% prob level for hurricane force 
  winds and waves, in green.
 07/31/2024: Ricardo M. Campos, mask up / de-emphasize the tropics between 30S to 30N.
 09/11/2024: Ricardo M. Campos, mask the tropics only during hurricane season. Generalize
  the code to be working with any domain (lat/lon intervals)
 08/18/2025: Ricardo M. Campos, in addition to GEFS, two new ensembles (ECMWF and EnvCanada)
  have been added. The code was modularized to facilitate.
 08/22/2025: Ricardo M. Campos, GML/S-413 figure format included.

PERSON OF CONTACT:
 Ricardo M Campos: ricardo.campos@noaa.gov

"""

# Pay attention to the pre-requisites and libraries
import matplotlib
matplotlib.use('Agg')
import xarray as xr
import matplotlib.pyplot as plt
import zipfile
import yaml
from matplotlib.colors import ListedColormap
from scipy.ndimage import gaussian_filter
import numpy as np
import pandas as pd
import cartopy
import cartopy.crs as ccrs
# Palette and colors for plotting the figures
import matplotlib.colors as colors
import sys
import warnings; warnings.filterwarnings("ignore")
# -------------------------
sl=13 # plot style configuration
matplotlib.rcParams.update({'font.size': sl}); plt.rc('font', size=sl) 
matplotlib.rc('xtick', labelsize=sl); matplotlib.rc('ytick', labelsize=sl); matplotlib.rcParams.update({'font.size': sl})
# --------------------------------------------------------------------------

# Reading functions
def read_gefs(auxltime,fvarname,nenm,mpath,fcdate,fchour):
    '''
    Read GEFS grib2 ensemble forecast files.
    '''

    # READ WW3 Ensemble Forecast files. Appending forecast days (time intervall).
    c=0
    for t in range(0,auxltime.shape[0]):
        for enm in range(0,nenm):

            fname=mpath+"GEFSv12Waves_"+fcdate+"/gefs.wave."+fcdate+"."+str(int(enm)).zfill(2)+".global.0p25.f"+str(int(auxltime[t])).zfill(3)+".grib2"

            if c==0:
                ds = xr.open_dataset(fname, engine='cfgrib')
                wtime = np.atleast_1d(np.array(ds.time.values))
                lat = np.array(ds.latitude.values); lat = np.sort(lat); lon = np.array(ds.longitude.values)
                fmod=np.zeros((auxltime.shape[0],nenm,lat.shape[0],lon.shape[0]),'f')*np.nan
                ds.close(); del ds

                if fvarname == 'Hs' or fvarname == 'HS':
                    xrvar='swh'
                else:
                    xrvar='ws'

            ds = xr.open_dataset(fname, engine='cfgrib')
            fmod[t,enm,:,:] = np.array(np.flip(ds[xrvar].values[:],axis=0)).astype('float')
            ds.close(); del ds

            c=c+1

        print(repr(t))

    return fmod, lat, lon, wtime

def read_mm(auxltime,model,mvar,nenm,mpath,fcdate,fchour):
    '''
    Read ECMWF and EnvCanada netcdf ensemble forecast files. 
    Downloaded and post-processed by 
      download_ECMWF_ENS_wave.sh or download_ECMWF_ENS_wind.sh  (ECMWF)
      download_GEWPS_wave.sh or download_GEPS_wind.sh (EnvCanada)
    '''
    import netCDF4 as nc

    # model informed in the .yaml configuration file
    if model=='ECMWF':
        mtag='ECMWF_ENS'
    elif model=='EnvCanada':
        mtag='GEWPS'

    c=0 # loop through the ensemble members
    for enm in range(0,nenm):
        # fname = mpath+fcdate+fchour+"/"+mtag+"_wave_"+fcdate+fchour+"."+str(enm).zfill(2)+".nc"
        fname = mpath+mtag+"_wave_"+fcdate+fchour+"."+str(enm).zfill(2)+".nc"
        try:
            f=nc.Dataset(fname)
            if mvar in list(f.variables.keys()):

                if c==0:
                    ftime = np.array(f.variables['time'][:]).astype('int')
                    indt = np.where(np.isin(ftime, auxltime))[0]
                    lat = np.sort(np.array(f.variables['lat'][:]))
                    lon = np.array(f.variables['lon'][:]); lon=lon+180.; half = len(lon) // 2
                    fmod = np.zeros((len(indt),nenm,len(lat),len(lon)),'f')

                    ds = xr.open_dataset(fname)
                    wtime = np.atleast_1d(np.array(ds.time.values))[indt]
                    del ds

                if fvarname.upper() == "WS10" or fvarname.upper() == "WND" or fvarname.upper() == "U10":
                    # u and v components
                    if c==0:
                        mvar=np.array(mvar.split(' ')).astype('str')

                    if model=='ECMWF':
                        auxu = np.flip(np.array(f.variables[mvar[0]][indt,:,:]), axis=1)
                        auxv = np.flip(np.array(f.variables[mvar[1]][indt,:,:]), axis=1)
                    else:
                        auxu = np.array(f.variables[mvar[0]][indt,:,:])
                        auxv = np.array(f.variables[mvar[1]][indt,:,:])

                    aux = np.sqrt(auxu**2 + auxv**2); del auxu,auxv

                else:
                    if model=='ECMWF':
                        aux = np.flip(np.array(f.variables[mvar][indt,:,:]), axis=1)
                    else:
                        aux = np.array(f.variables[mvar][indt,:,:])

                aux = np.roll(aux, shift=half, axis=2)
                aux[aux<-100]=np.nan; aux[aux>100]=np.nan
                fmod[:,enm,:,:] = aux
                del aux

            else:
                sys.exit(" Enviromental variable "+mvar+" not found in the file "+fname)

        except:
            sys.exit(" Could not open and read file "+fname)
        else:
            c=c+1

    return fmod, lat, lon, wtime


def stprob(fmod,nmax,nenm,lat,lon,spws,qlev,spctl):
    '''
    Space-Time Cells and Probabilities.
    '''

    # n-max expansion and reshape
    fmod=np.sort(fmod,axis=0)
    fmod=np.copy(fmod[-nmax::,:,:,:])
    fmod=np.array(fmod.reshape(nmax*nenm,lat.shape[0],lon.shape[0]))

    gspws=int(np.floor(spws/np.diff(lat).mean())/2)

    probecdf=np.zeros((qlev.shape[0],lat.shape[0],lon.shape[0]),'f')
    for i in range(0,qlev.shape[0]):
        for j in range(0,lat.shape[0]):
            for k in range(0,lon.shape[0]):
                if np.any(fmod[:,j,k]>0.0):
                    if (j>=gspws) and (j<=lat.shape[0]-gspws) and (k>=gspws) and (k<=lon.shape[0]-gspws):
                        aux=np.array(fmod[:,(j-gspws):(j+gspws+1),:][:,:,(k-gspws):(k+gspws+1)])
                        aux=aux.reshape(nmax*nenm,aux.shape[1]*aux.shape[2])
                        aux=np.sort(aux,axis=1)
                        ind=np.where(np.mean(aux,axis=0)>=0.)
                        if np.size(ind)>0:
                            aux=np.array(aux[:,ind[0]])            
                            aux=np.array(aux[:,int(np.floor(aux.shape[1]*(spctl/100)))::])
                            aux=aux.reshape(aux.shape[0]*aux.shape[1])
                            probecdf[i,j,k] = np.size(aux[aux>qlev[i]]) / np.size(aux[aux>0.])

                        del aux

    return probecdf


# Plot functions

def pctl_plot(fmod,auxltime,nenm,lat,lon,pctls,slonmin,slonmax,slatmin,slatmax,spws,outpath,model,fvarname,ltime1,ltime2):
    '''
    Percentile plots.
    '''

    fmodo=np.array(fmod.reshape(auxltime.shape[0]*nenm,lat.shape[0],lon.shape[0]))
    wlevels=np.linspace(0,np.nanpercentile(fmod,99.99),101)
    ftag=model

    # Percentiles informed in the .yaml configuration file.
    for i in range(0,pctls.shape[0]):
        plt.figure(figsize=(9,5.5))
        ax = plt.axes(projection=ccrs.PlateCarree(central_longitude=-90))
        ax.set_extent([slonmin+spws,slonmax-spws,slatmin+spws,slatmax-spws], crs=ccrs.PlateCarree())
        # ax.set_extent([slonmin,slonmax,slatmin,slatmax], crs=ccrs.PlateCarree())
        gl = ax.gridlines(crs=ccrs.PlateCarree(),  xlocs=range(-180,180, 20), draw_labels=True, linewidth=0.5, color='grey', alpha=0.5, linestyle='--')
        gl.xlabel_style = {'size': 9, 'color': 'k','rotation':0}; gl.ylabel_style = {'size': 9, 'color': 'k','rotation':0}
        cs=ax.contourf(lon,lat,np.nanpercentile(fmodo,pctls[i],axis=0),levels=wlevels,alpha=0.7,cmap='jet',zorder=1,extend="max",transform = ccrs.PlateCarree())
        ax.add_feature(cartopy.feature.OCEAN,facecolor=("white"))
        ax.add_feature(cartopy.feature.LAND,facecolor=("lightgrey"), edgecolor='grey',linewidth=0.5, zorder=2)
        ax.add_feature(cartopy.feature.BORDERS, edgecolor='grey', linestyle='-',linewidth=0.5, alpha=1, zorder=3)
        ax.coastlines(resolution='50m', color='dimgrey',linewidth=0.5, linestyle='-', alpha=1, zorder=4)
        title = "Percentile"+str(pctls[i]).zfill(2)+" "+fvarname+" ("+funits+"), Cycle "+fcycle[0:8]+" "+fcycle[8:10]+"Z \n"
        title += r"$\bf{"+trfi+"Valid: "+pd.to_datetime(wtime[0]+np.timedelta64(ltime1,'D')).strftime('%B %d, %Y')+" - "
        title += pd.to_datetime(wtime[0]+np.timedelta64(ltime2,'D')).strftime('%B %d, %Y')+"}$"
        ax.set_title(title); del title
        plt.tight_layout()
        ax = plt.gca(); pos = ax.get_position(); l, b, w, h = pos.bounds; cax = plt.axes([l+0.06, b-0.07, w-0.12, 0.03]) # setup colorbar axes.
        cbar = plt.colorbar(cs,cax=cax, orientation='horizontal', format='%g')
        labels = np.arange(0, wlevels.max(),vtickd).astype('int'); ticks = np.arange(0, wlevels.max(),vtickd).astype('int')
        cbar.set_ticks(ticks); cbar.set_ticklabels(labels)
        plt.axes(ax); plt.tight_layout()
        plt.text(-90., 76., 'Experimental', color='k', fontsize=13, fontweight='bold')
        figname = outpath+"Pctl"+str(pctls[i]).zfill(2)+"_"+fvarname+"_fcst"+str(ltime1).zfill(2)+"to"+str(ltime2).zfill(2)+"_"+ftag
        plt.savefig(figname+".png", dpi=200, facecolor='w', edgecolor='w',
            orientation='portrait', format='png',transparent=False, bbox_inches='tight', pad_inches=0.1)

        plt.close('all')
        # convert to GML/S-413 format
        # with zipfile.ZipFile(figname+".kmz", "w") as kmz:
        #    kmz.write(figname+".png")

        del ax, figname
        print(" 2. Initial Plots. Percentile "+str(pctls[i]).zfill(2)+" ok.")


def pm_plot(probecdf,wtime,wconfig,mode,spws,plevels,qlev,hplevels,hpcolors,pcolors,lat,lon,slonmin,slonmax,slatmin,slatmax,fvarname,funits,model,fcycle,outpath,ltime1,ltime2):
    '''
    Probability Maps.
    '''
    ftag=model

    # Probability levels and labels for the plots
    clabels=[];clevels=[]
    for j in range(0,np.size(plevels)-1):
        clabels=np.append(clabels,">"+str(int(plevels[j]*100)).zfill(2)+"%")
        clevels=np.append(clevels,(plevels[j]+plevels[j+1])/2)

    cmap = ListedColormap(pcolors)

    # If hurricane season (May 15th to Nov 31st), it masks (or cuts) the tropics for external plots
    mmdd = wtime[0].astype('datetime64[D]').astype(object).month * 100 + wtime[0].astype('datetime64[D]').astype(object).day
    if mode=='external' and (mmdd>=515 and mmdd<=1130):
        slatmin=wconfig['latminhs']-spws # min latitude for the hurricane season

    # Loop through the intensity levels
    for i in range(0,qlev.shape[0]):

        # Extra percentage level associated with the most extreme case
        if i == int(qlev.shape[0]-1):

            plevels = hplevels
            pcolors = hpcolors

            clabels=[];clevels=[]
            for j in range(0,np.size(plevels)-1):
                clabels=np.append(clabels,">"+str(int(plevels[j]*100))+"%")
                clevels=np.append(clevels,(plevels[j]+plevels[j+1])/2)

            cmap = ListedColormap(pcolors)

        # Figure
        plt.figure(figsize=(9,5.5))
        ax = plt.axes(projection=ccrs.PlateCarree(central_longitude=-90))
        ax.set_extent([slonmin+spws,slonmax-spws,slatmin+spws,slatmax-spws], crs=ccrs.PlateCarree())
        # ax.set_extent([slonmin,slonmax,slatmin,slatmax], crs=ccrs.PlateCarree())
        gl = ax.gridlines(crs=ccrs.PlateCarree(),  xlocs=range(-180,180, 20), draw_labels=True, linewidth=0.5, color='grey', alpha=0.5, linestyle='--')
        gl.xlabel_style = {'size': 9, 'color': 'k','rotation':0}; gl.ylabel_style = {'size': 9, 'color': 'k','rotation':0}
        ax.add_feature(cartopy.feature.OCEAN,facecolor=("white"))
        ax.add_feature(cartopy.feature.LAND,facecolor=("lightgrey"), edgecolor='grey',linewidth=0.5, zorder=2)
        ax.add_feature(cartopy.feature.BORDERS, edgecolor='grey', linestyle='-',linewidth=0.5, alpha=1, zorder=3)
        ax.coastlines(resolution='50m', color='dimgrey',linewidth=0.5, linestyle='-', alpha=1, zorder=4)
        title = "Prob "+fvarname+">"+str(qlev[i]).zfill(1)+funits+", Cycle "+fcycle[0:8]+" "+fcycle[8:10]+"Z \n"
        title += r"$\bf{"+trfi+"Valid: "+pd.to_datetime(wtime[0]+np.timedelta64(ltime1,'D')).strftime('%B %d, %Y')+" - "
        title += pd.to_datetime(wtime[0]+np.timedelta64(ltime2,'D')).strftime('%B %d, %Y')+"}$"

        # If external and in the hurricane season, it masks (or cuts) the tropics.
        if mode=='external' and (mmdd>=515 and mmdd<=1130) and np.any((np.linspace(wconfig['latminhs'],slatmax,10)>-25.) & (np.linspace(wconfig['latminhs'],slatmax,10)<25.)):
            contour_data = gaussian_filter(probecdf[i, :, :], gft)
            tropical_mask = (lat >= -30) & (lat <= 30)
            # Expand tropical_mask to match the shape of contour_data
            tropical_mask_expanded = np.tile(tropical_mask[:, np.newaxis], (1, lon.shape[0]))
            # Add transparency to tropical areas
            cs = ax.contourf(lon, lat, np.ma.masked_where(tropical_mask_expanded, contour_data), 
                                          levels=plevels, cmap=cmap, alpha=0.7, zorder=1, transform=ccrs.PlateCarree())

            ax.add_patch(plt.Rectangle((slonmin, -30), 360, 60, transform=ccrs.PlateCarree(), color='whitesmoke', alpha=0.4, zorder=2, hatch='//', edgecolor='none'))
            ax.add_patch(plt.Rectangle((slonmin, -30), 360, 60, transform=ccrs.PlateCarree(), color='whitesmoke', alpha=0.4, zorder=2, hatch='\\', edgecolor='none'))

            plt.text(slonmin+spws-270+1, 0., 'This product is not intended for tropical cyclones', color='k', fontsize=8, bbox=dict(facecolor='white', alpha=0.8, edgecolor='none'), zorder=4)
            plt.text(slonmin+spws-270+1, -5., 'Tropical latitudes are masked during hurricane season.', color='k', fontsize=7, bbox=dict(facecolor='white', alpha=0.8, edgecolor='none'),zorder=4)

        else:
            cs=ax.contourf(lon,lat,gaussian_filter(probecdf[i,:,:],gft),levels=plevels,alpha=0.7,cmap=cmap,zorder=1,transform = ccrs.PlateCarree())

        ax.set_title(title); del title
        plt.tight_layout()
        ax = plt.gca(); pos = ax.get_position(); l, b, w, h = pos.bounds; cax = plt.axes([l+0.06, b-0.07, w-0.12, 0.03]) # setup colorbar axes.
        cbar = plt.colorbar(cs,cax=cax, orientation='horizontal',ticks=clevels, format='%g')
        cbar.ax.set_xticklabels(clabels)
        cbar.ax.tick_params(length=0)
        for label in cbar.ax.get_xticklabels():
            label.set_weight('bold')

        plt.axes(ax); plt.tight_layout()
        plt.text(-90., 76., 'Experimental', color='k', fontsize=13, fontweight='bold')
        figname = outpath+"ProbMap_"+fvarname+"_"+str(qlev[i]).zfill(1)+"_fcst"+str(ltime1).zfill(2)+"to"+str(ltime2).zfill(2)+"_"+mode+"_"+ftag
        plt.savefig(figname+".png", dpi=200, facecolor='w', edgecolor='w',
                orientation='portrait', format='png',transparent=False, bbox_inches='tight', pad_inches=0.1)

        plt.close('all')

        del ax, figname
        print("   Plot ... qlev "+repr(qlev[i]))

# ------------------------------------------------------------


if __name__ == "__main__":

    # Input Arguments -----
    # .yaml configuration file
    fconfig=str(sys.argv[1])
    # Forecast Cycle
    fcycle=str(sys.argv[2])
    fcdate=str(fcycle[0:8]); fchour=str(fcycle[8:10])
    # Forecast Lead Time (Day) and intervall
    ltime1=int(sys.argv[3])
    ltime2=int(sys.argv[4])
    # forecast variable: WS10, Hs
    fvarname=str(sys.argv[5])

    # Fixed configuration variables, read yaml file -----------
    print(" "); print(" Reading yaml configuration file ...")
    with open(fconfig, 'r') as file:
        wconfig = yaml.safe_load(file)

    model=str(wconfig['model'])

    # number of ensemble members
    mode=str(wconfig['mode'])
    # number of ensemble members
    nenm=wconfig['nenm']
    # time resolution
    tres=wconfig['tres']
    # n-max expansion
    nmax=wconfig['nmax']
    # spatial window size (diameter), square (degrees)
    spws=wconfig['spws']
    # Plotting Area
    slonmin=wconfig['lonmin']-spws; slonmax=wconfig['lonmax']+spws
    slatmin=wconfig['latmin']-spws; slatmax=wconfig['latmax']+spws
    # Spatial percentile (grid points)
    spctl=wconfig['spctl']
    # gaussian filter spatial plot smoothing
    gft=wconfig['gft']
    # percentiles for the initial plots
    pctls=np.array(wconfig['pctls']).astype('int')
    # Probability levels
    plevels = np.array(wconfig['plevels']).astype('float')
    hplevels = np.array(wconfig['hplevels']).astype('float')
    # Colors for the probability maps
    pcolors = np.array(wconfig['pcolors']).astype('str')
    hpcolors = np.array(wconfig['hpcolors']).astype('str')

    # Path of Forecast files
    mpath=wconfig['mpath']
    if mpath[-1] != '/':
        mpath=mpath+"/"

    # output path
    outpath=str(wconfig['outpath'])
    if outpath[-1] != '/':
        outpath=outpath+"/"

    umf=1. # unit conversion, when necessary

    # maximum value allowed (quick quality control), variable name (grib2), and levels for the probability plot
    if fvarname.upper() == "WS10" or fvarname.upper() == "WND" or fvarname.upper() == "U10":
        qqvmax=wconfig['qqvmax_wnd']
        mvar=wconfig['mvar_wnd']
        qlev=np.array(wconfig['qlev_wnd']).astype('float')
        vtickd=int(wconfig['vtickd_wnd'])
        funits=str('knots')
        umf=1.94 # m/s to knots
    elif fvarname.upper() == "HS":
        qqvmax=wconfig['qqvmax_hs']
        mvar=wconfig['mvar_hs']
        funits=str('m')
        qlev=np.array(wconfig['qlev_hs']).astype('float')
        vtickd=int(wconfig['vtickd_hs'])
    else:
        sys.exit(" Input variable "+fvarname+" not included in the list. Please select only one: WS10, Hs.")

    print(" Reading yaml configuration file, OK."); print(" ")

    # Time range forecast intervall string, for the plots
    if (ltime2-ltime1)<0:
        aux=np.copy(ltime1); ltime1=np.copy(ltime2)
        ltime2=np.copy(aux); del aux

    if ltime1==1 and (ltime2-ltime1)==7:
        trfi=str("Week 1 - ")
    elif ltime1==7 and (ltime2-ltime1)==7:
        trfi=str("Week 2 - ")
    elif ltime1==14 and (ltime2-ltime1)==7:
        trfi=str("Week 3 - ")
    elif ltime1==21 and (ltime2-ltime1)==7:
        trfi=str("Week 4 - ")
    elif ltime1==28 and (ltime2-ltime1)==7:
        trfi=str("Week 5 - ")
    elif (ltime2-ltime1)>0:
        trfi=str("Days "+str(ltime1)+"-"+str(ltime2)+" , ")
    elif ltime2==ltime1:
        trfi=str("Day "+str(ltime1)+" , ")
    else:
        trfi=''

    print(" "); print(" 1. Reading Forecast Data ...")

    auxltime = np.arange((ltime1-1)*24,((ltime2)*24)+1,tres)
    auxltime[auxltime>384]=384; auxltime[auxltime<0]=0

    # READ Ensemble Forecast files. Appending forecast days (time intervall).
    if model=='GEFS':
        fmod, lat, lon, wtime = read_gefs(auxltime,fvarname,nenm,mpath,fcdate,fchour)
    else:
        fmod, lat, lon, wtime = read_mm(auxltime,model,fvarname,nenm,mpath,fcdate,fchour)

    # Quick simple quality control
    fmod[fmod>=qqvmax]=np.nan; fmod[fmod<0.]=np.nan
    # Unit conversion
    fmod=fmod*umf
    # Select domain of interest
    indlat=np.where((lat>=(slatmin-spws))&(lat<=(slatmax+spws)))
    indlon=np.where((lon>=(slonmin-spws))&(lon<=(slonmax+spws)))
    if np.size(indlon)>0 and np.size(indlat)>0:
        lat=np.copy(lat[indlat[0]]); lon=np.copy(lon[indlon[0]])
        fmod=np.copy(fmod[:,:,indlat[0],:][:,:,:,indlon[0]])
    else:
        sys.exit(" Min/Max lat and lon incorrect when applied to the forecast file. Check longitude standards.")

    del indlat, indlon
    print(" 1. Forecast Data ... OK"); print(" ")


    print(" 2. Initial Plots ...")
    pctl_plot(fmod,auxltime,nenm,lat,lon,pctls,slonmin,slonmax,slatmin,slatmax,spws,outpath,model,fvarname,ltime1,ltime2)

    print(" "); print(" 3. Space-Time Cells and Probabilities ...")
    probecdf = stprob(fmod,nmax,nenm,lat,lon,spws,qlev,spctl)
    print(" 3. Space-Time Cells and Probabilities ... OK"); print(" ")

    # MAIN PLOTS
    print(" 4. Probability Maps ...")
    pm_plot(probecdf,wtime,wconfig,mode,spws,plevels,qlev,hplevels,hpcolors,pcolors,lat,lon,slonmin,slonmax,slatmin,slatmax,fvarname,funits,model,fcycle,outpath,ltime1,ltime2)
    print(" 4. Probability Plots ... OK"); print(" ")


