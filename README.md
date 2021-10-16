# BeatSaberScripts

An attempt to automatically build a workout playlist.

Requires BeatSaviourData (for tracking speed) and supports SongPlayHistoryContinued.

Operates using 3 playlists with fixed filenames:

* workout.bplist - a list of songs in ascending order of hand speed
* unplayed.bplist - songs with a predicted score of >75% which have no record of being played by BeatSaviorData or SongPlayHistoryCombined
* bannedforworkout.bplist - a list of songs not to include
