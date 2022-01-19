#!perl

use warnings;
use strict;

use v5.16;

use JSON qw(decode_json);
use File::Spec::Functions qw(catdir catfile updir rel2abs);
use Cwd qw(abs_path);
use LWP::RobotUA;
use HTTP::Cache::Transparent;
use DBD::SQLite;
use DBI;
use POSIX qw(ceil floor strftime);
use File::Find;
use File::Path qw(make_path);
use File::Slurp;
use LWP::ConnCache;
use Carp qw(croak);
use English qw(-no_match_vars);
use Archive::Zip;
use Date::Parse;
use Statistics::Regression;
use Time::HiRes qw(time sleep);

use autodie qw(:all);

my $DEBUG = 0;
$DEBUG = 1 if @ARGV;

my $PlayerID = '76561198001262880';

my $NOW = time();

my $DataDir = catdir(abs_path(__FILE__), updir(), "data");

my $CacheDir = catdir($DataDir, "cache");

my $BeatSaberFolder = "G:\\Steam\\steamapps\\common\\Beat Saber";
my $BeatSaberFolders = "G:\\Modding\\Beat Saber";

my $BeatSaberAppdataFolder = catdir($ENV{localappdata},updir(),"LocalLow","Hyperbolic Magnetism","Beat Saber");

my $PlaylistFolder = catdir($BeatSaberFolder, "Playlists");

my $SongsFolder = catdir($BeatSaberFolder, "Beat Saber_Data", "CustomLevels");

# TODO figure out a better way than this.
my ($SameSongs) = {
  "custom_level_B68BF61AC6BE0E128BE32A85810D42E7C53F4756" => "BeatSaber",
  "custom_level_45F9480A43DDEA9FF338BF449AD9EAD62F73EB52" => "custom_level_2C002D2874E029DB43F3C7CF9BB271AE0D769B74",
  "custom_level_E6AF862558EF500F16D3B82161BBDFB1D2C296BF" => "custom_level_2C002D2874E029DB43F3C7CF9BB271AE0D769B74",
  "custom_level_A03CB0A107993BE5CDED1E91DC31E8A7B048F02A" => "custom_level_88A33C64E8FDE2A88D9A99799B9F58205C412B10",
};

sub ts {
  return strftime("%Y-%m-%dT%H:%M:%S", localtime());
}

sub ts2 {
  return strftime("%Y-%m-%dT%H%M%S", localtime());
}

do {
  my $json = JSON->new->pretty->canonical;

  sub encodeJson { return $json->encode(@_); }
};

my $SECONDS_PER_DAY = 24 * 60 * 60;
my $SECONDS_IN_15_MINUTES = 15 * 60;
my $SECONDS_IN_SIX_MONTHS = 6 * 30 * $SECONDS_PER_DAY;

HTTP::Cache::Transparent::init({
  BasePath => catdir($CacheDir, "http"),
  MaxAge => 8*24,
  Verbose => 1,
  NoUpdate => ($DEBUG ? $SECONDS_PER_DAY : $SECONDS_IN_15_MINUTES),
  ApproveContent => sub { return $_[0]->is_success && length($_[0]->content) > 0 }
});

my $connCache = LWP::ConnCache->new();

$connCache->total_capacity(undef);

my $ua = LWP::RobotUA->new(
  agent => 'buildBeatSaberPlaylist/0.1 ',
  from => 'Martin Rudat <mrudat@toraboka.com>',
  # allowed to make at most 400 request/minute
  delay => 1/400,
  conn_cache => $connCache
);

$ua->use_sleep(1);

$ua->default_header(
  'Accept-Encoding' => 'application/json'
);

sub get2 {
  my ($url) = @_;

  my ($then) = time();

  my $res = $ua->get($url);
  my $elapsed = time() - $then;
  say "took ${elapsed}s, sleeping for ${elapsed}s";
  sleep($elapsed);

  croak $res->as_string unless $res->is_success;

  return $res->content;
}

sub get {
  my ($url) = @_;
  my $res;
  my $retry_count = 1;
  do {
    eval {
      $res = get2($url);
    };
    if ($EVAL_ERROR) {
      if ($EVAL_ERROR =~ m/^500 read timeout/) {
        sleep(5 * $retry_count);
      } else {
        die $EVAL_ERROR;
      }
    }
    return $res;
  } while ($retry_count++ < 5);
  die $EVAL_ERROR;
}


my $FAVOURITES;

sub loadFavourites {
  my $playerDataFile = catfile($BeatSaberAppdataFolder, "PlayerData.dat");
  if (-f $playerDataFile) {
    my $playerData = decode_json(read_file($playerDataFile));

    foreach my $levelId (@{$playerData->{localPlayers}[0]{favoritesLevelIds}}) {
      $FAVOURITES->{$levelId} = 1;
      my $songId = $levelId;
      while (exists $SameSongs->{$songId}) {
        $songId = $SameSongs->{$songId};
        $FAVOURITES->{$songId} = 1;
      }
    }
  }
}

our $MY_SCORES;
our $LEADERBOARDS;
my $ranks;
my $StarsToAccuracy = Statistics::Regression->new("Stars to accuracy",[qw(s^3 s^2 s c)]);

sub fetchScores {
  my $baseUrl = "https://scoresaber.com/api/player/${PlayerID}/scores?sort=recent";

  my $res = decode_json(get($baseUrl . "&withMetadata=true&limit=100"));

  my $page = 1;
  my $done = 0;

  my $itemsPerPage = $res->{metadata}{itemsPerPage};

  $baseUrl .= "&withMetadata=false&limit=${itemsPerPage}";

  my $pages = ceil($res->{metadata}{total} / $itemsPerPage);

  while (1) {
    my $scores = $res->{playerScores};

    foreach my $rec (@{$scores}) {
      my $score = $rec->{score};
      my $leaderboard = $rec->{leaderboard};

      my $leaderboardId = $leaderboard->{id};

      my $timeSet = str2time($score->{timeSet});
      my $age = ($NOW - $timeSet) / $SECONDS_PER_DAY;

      $MY_SCORES->{$leaderboardId} = $score;
      $LEADERBOARDS->{$leaderboardId} = $leaderboard;

      my $stars = $leaderboard->{stars};
      if ($stars > 0) {
        my $maxScore = $leaderboard->{maxScore};
        my $accuracy = $score->{modifiedScore} / $maxScore;
        $StarsToAccuracy->include($accuracy, [$stars ** 3, $stars ** 2, $stars, 1.0], 1.0 / $age);
      }

      $ranks->{$leaderboardId} = $score->{rank};
    }

    $page++;
    last if $page > $pages;

    $res = decode_json(get($baseUrl . "&page=$page"));
  }

  eval {
    $StarsToAccuracy->print();
  };
}

# number of leaderboards we share with player.
our $NEIGHBOURS;
# number of leaderboards of songs we like that we share with player.
our $LIKED_NEIGHBOURS;
our $NEIGHBOUR_SCORE_DATE;

sub fetchLeaderboards {
  my $itemsPerPage = 12;

  my $checkItemsPerPage = 1;

  my $leaderboardPage;

  foreach my $leaderboard (values %{$LEADERBOARDS}) {
    my $id = $leaderboard->{id};
    my $rank = $ranks->{$id};
    my $myScore = $MY_SCORES->{$id}{modifiedScore};
    my $hash = $leaderboard->{songHash};
    my $levelId = "custom_level_" . $hash;
    my $liked = 0;
    $liked = 1 if exists $FAVOURITES->{$levelId};
    if ($liked) {
      say "Considering leaderboard ${id} of song we like where we have rank #${rank} with score ${myScore}";
    } else {
      say "Considering leaderboard ${id} where we have rank #${rank} with score ${myScore}";
    }

    my $page = ceil($rank / $itemsPerPage);

    my $res = get("https://scoresaber.com/api/leaderboard/by-id/${id}/scores?page=${page}");

    next if length($res) == 0;

    $res = decode_json($res);

    if ($checkItemsPerPage) {
      my $newItemsPerPage = $res->{metadata}{itemsPerPage};
      if ($newItemsPerPage != $itemsPerPage) {
        say "itemsPerPage for scores is now $newItemsPerPage!";
        $itemsPerPage = $newItemsPerPage;

        $page = ceil($rank / $itemsPerPage);

        $res = decode_json(get("https://scoresaber.com/api/leaderboard/by-id/${id}/scores?page=${page}"));
      }
      $checkItemsPerPage = 0;
    }

    my $scores = $res->{scores};

    my $found = 0;

    foreach my $rec (@{$scores}) {
      my $playerId = $rec->{leaderboardPlayerInfo}{id};
      # this is us, we're not our own neighbour.
      next if $playerId == $PlayerID;
      my $score = $rec->{modifiedScore};
      # old record?
      $score = $rec->{modifiedScore} if $score == 0;
      my $scoreAdjust = $myScore / $score;
      # if the other player's score is too different it's unlikely they're a useful estimate of our skill on other maps.
      next if (($scoreAdjust > 1.2) || ($scoreAdjust < 0.8));
      $NEIGHBOUR_SCORE_DATE->{$playerId}{$id} = [ 
        str2time($rec->{timeSet}),
        $scoreAdjust 
      ];
      $NEIGHBOURS->{$playerId}++;
      $LIKED_NEIGHBOURS->{$playerId} += $liked;
      $found = 1;
    }

    if ($found && ($page > 1)) {
      $leaderboardPage->{$id} = $page - 1;
    }
  }
}

my $NEW_SONGS;
my $NEW_SONGS_WEIGHT;
my $SONGS_TO_IMPROVE;
my $OLD_SONGS;
my $NEIGHBOUR_SCORE;
my $NEW_SONGS_NEIGHBOUR_SCORE;

sub fetchNeighbours {
  my $playerCount = 0;
  foreach my $neighbour (sort { 
    $LIKED_NEIGHBOURS->{$b} <=> $LIKED_NEIGHBOURS->{$a}
    ||
    $NEIGHBOURS->{$b} <=> $NEIGHBOURS->{$a}
  } keys %{$NEIGHBOURS}) {
    my $neighbourCount = $NEIGHBOURS->{$neighbour};
    my $likedNeighbourCount = $LIKED_NEIGHBOURS->{$neighbour};
    # pick only those people that appear frequently.
    last if $neighbourCount <= 2;
    last if $playerCount++ >= 200;
    say "Considering player $neighbour who we share $neighbourCount leaderboards with and $likedNeighbourCount leaderboards of songs we like";
    my $likedWeight = $likedNeighbourCount / $neighbourCount;

    my $res = decode_json(get("https://scoresaber.com/api/player/${neighbour}/scores?limit=100&sort=top"));

    my $scores = $res->{playerScores};

    my $scoreDates = $NEIGHBOUR_SCORE_DATE->{$neighbour};
    $scoreDates = [values %{$scoreDates}];

    foreach my $rec (@{$scores}) {
      my $score = $rec->{score};
      my $leaderboard = $rec->{leaderboard};

      my $timeSet = str2time($score->{timeSet});

      my $delta;
      my $scoreAdjust;
      foreach my $scoreDate (@{$scoreDates}) {
        my $temp = abs($scoreDate->[0] - $timeSet);
        if ((not defined $delta) || ($temp < $delta)) {
          $delta = $temp;
          $scoreAdjust = $scoreDate->[1];
        };
      }

      $delta -= $SECONDS_IN_SIX_MONTHS;
      $delta = 0 if $delta > 0;
      $delta /= -$SECONDS_IN_SIX_MONTHS;
      # delta is now in the range 0 .. 1, with 0 being a difference of 6 months and a 1 being a difference of 0 seconds.

      my $leaderboardId = $leaderboard->{id};

      # TODO weight the average both on age of the score relative to the scores on the shared leaderboards and the number of shared leaderboards

      if (exists $LEADERBOARDS->{$leaderboardId}) {
        $NEIGHBOUR_SCORE->{$leaderboardId}{total} += $delta * $score->{modifiedScore} * $scoreAdjust;
        $NEIGHBOUR_SCORE->{$leaderboardId}{totalWeight} += $delta;
      } else {
        $NEW_SONGS->{$leaderboardId} = $leaderboard;
        $NEW_SONGS_WEIGHT->{$leaderboardId} += $delta * $likedWeight;
        $NEW_SONGS_NEIGHBOUR_SCORE->{$leaderboardId}{total} += $delta * $score->{modifiedScore} * $scoreAdjust;
        $NEW_SONGS_NEIGHBOUR_SCORE->{$leaderboardId}{totalWeight} += $delta;
      }
    }
  }

  foreach my $leaderboardId (keys %{$NEIGHBOUR_SCORE}) {
    my $scores = $NEIGHBOUR_SCORE->{$leaderboardId};
    my $totalWeight = $scores->{totalWeight};
    my $averageModifiedScore = 0;
    if ($totalWeight > 0) {
      $averageModifiedScore = floor($scores->{total} / $totalWeight);
    }
    my $myModifiedScore = $MY_SCORES->{$leaderboardId}{modifiedScore};
    if ($averageModifiedScore > $myModifiedScore) {
      my $data = $LEADERBOARDS->{$leaderboardId};
      $data->{potentialScore} = $averageModifiedScore;
      $data->{potentialImprovement} = ($averageModifiedScore - $myModifiedScore) / $myModifiedScore;
      $SONGS_TO_IMPROVE->{$leaderboardId} = $data;
      $OLD_SONGS->{$leaderboardId} = $data;
    } else {
      my $data = $LEADERBOARDS->{$leaderboardId};
      $data->{potentialScore} = $averageModifiedScore;
      $OLD_SONGS->{$leaderboardId} = $data;
    }
    $NEIGHBOUR_SCORE->{$leaderboardId} = $averageModifiedScore;
  }

  foreach my $leaderboardId (keys %{$NEW_SONGS_NEIGHBOUR_SCORE}) {
    my $scores = $NEW_SONGS_NEIGHBOUR_SCORE->{$leaderboardId};
    my $totalWeight = $scores->{totalWeight};
    my $averageModifiedScore = 0;
    if ($totalWeight > 0) {
      $averageModifiedScore = floor($scores->{total} / $totalWeight);
    }
    my $data = $NEW_SONGS->{$leaderboardId};
    $data->{potentialScore} = $averageModifiedScore;
    $NEW_SONGS_NEIGHBOUR_SCORE->{$leaderboardId} = $averageModifiedScore;
  }
}

my $difficultyMap = {
  1 => "Easy",
  3 => "Normal",
  5 => "Hard",
  7 => "Expert",
  9 => "ExpertPlus"
};

my $OptionalPlaylistKeys = [qw(
  potentialImprovement
  potentialScore
  potentialAccuracy
)];

sub writePlaylist {
  my ($songData, $file_name, $title) = @_;

  my $songs;

  foreach my $data (@{$songData}) {
    my $gameMode = $data->{difficulty}{gameMode};
    next unless $gameMode =~ s/^Solo//;

    my $playlistEntry = {
      hash => $data->{songHash},
      levelid => "custom_level_" . $data->{songHash},
      difficulties => [
        {
          characteristic => $gameMode,
          name => $difficultyMap->{$data->{difficulty}{difficulty}}
        }
      ],
      songName => $data->{songName},
      songSubName => $data->{songSubName},
      songAuthorName => $data->{songAuthorName},
      levelAuthorName => $data->{levelAuthorName}
    };

    foreach my $key (@{$OptionalPlaylistKeys}) {
      $playlistEntry->{$key} = $data->{$key} if exists $data->{$key};
    }

    push @{$songs}, $playlistEntry;
  }

  my $playlist = {
    playlistTitle => $title,
    playlistAuthor => $0,
    songs => $songs,
    image => "",
  };

  write_file(catfile($PlaylistFolder, $file_name), encodeJson($playlist));
}

my $SONG_HASH_TO_PATH;

sub loadSongHashData {
  return if (defined $SONG_HASH_TO_PATH);
  my $songHashFile = catfile($BeatSaberFolder, "UserData", "SongCore", "SongHashData.dat");

  if (-f $songHashFile) {
    my $songHashData = decode_json(read_file($songHashFile));
    foreach my $dir (keys %{$songHashData}) {
      $SONG_HASH_TO_PATH->{uc $songHashData->{$dir}{songHash}} = $dir;
    }
  }

}

my $SONG_INFO;

sub getSongInfo {
  my ($songHash) = @_;
  my $songInfo;

  if (exists $SONG_INFO->{$songHash}) {
    $songInfo = $SONG_INFO->{$songHash};
  } else {
    my $songPath = $SONG_HASH_TO_PATH->{$songHash};
    if ($songPath && -d $songPath) {
      my $songInfoPath = catfile($songPath, "Info.dat");
      if (-f $songInfoPath) {
        $songInfo = decode_json(read_file($songInfoPath));
        $songInfo->{songPath} = $songPath;
        $SONG_INFO->{$songHash} = $songInfo;
      }
    }
  }
  return $songInfo;
}

my $NEW_SONG_DATA;

sub saveNewSongs {
  loadSongHashData();

  my $newSongs;

  my $downloaded = 0;

  foreach my $leaderboardId (sort { $NEW_SONGS_WEIGHT->{$b} <=> $NEW_SONGS_WEIGHT->{$a} } keys %{$NEW_SONGS_WEIGHT}) {
    my $data = $NEW_SONGS->{$leaderboardId};
    my $hash = uc $data->{songHash};

    my $gameMode = $data->{difficulty}{gameMode};
    next unless $gameMode =~ s/^Solo//;
    my $difficultyRank = $data->{difficulty}{difficulty};
    my $difficulty = $difficultyMap->{$difficultyRank};

    my $noteCount;
    my $duration;
    my $downloadURL;
    my $songDirectory;

    my $songInfo = getSongInfo($hash);

    if ($songInfo) {
      my $songPath = $songInfo->{songPath};
      BEAT_MAP_SET: foreach my $beatMapSet (@{$songInfo->{_difficultyBeatmapSets}}) {
        next unless $beatMapSet->{_beatmapCharacteristicName} eq $gameMode;
        foreach my $beatMapData (@{$beatMapSet->{_difficultyBeatmaps}}) {
          next unless $difficultyRank == $beatMapData->{_difficultyRank};
          my $beatMapPath = catfile($songPath,$beatMapData->{_beatmapFilename});
          last BEAT_MAP_SET unless -f $beatMapPath;

          my $beatmap = decode_json(read_file($beatMapPath));
          my $notes = $beatmap->{_notes};
          $noteCount = $#{$notes} + 1;
          my $secondsPerBeat = 60 / $songInfo->{_beatsPerMinute};
          $duration = $notes->[-1]{_time} * $secondsPerBeat;
          last BEAT_MAP_SET;
        }
      }
      warn "Couldn't find note count for $gameMode/$difficultyRank from $songPath" if not defined $noteCount;
    }

    if (not defined $noteCount) {
      next if $downloaded >= 10;
      my $res;
      eval {
        $res = get("https://api.beatsaver.com/maps/hash/$hash");
      };
      if ($EVAL_ERROR) {
        if ($EVAL_ERROR =~ m/404 Not Found/) {
          warn $EVAL_ERROR;
          next;
        }
        die $EVAL_ERROR;
      }
      $res = decode_json($res);
      my $versions = $res->{versions};
      VERSION: foreach my $version (@{$versions}) {
        next unless $version->{state} eq "Published";
        next unless $hash eq uc $version->{hash};
        $downloadURL = $version->{downloadURL};
        my $id = $res->{id};
        my $songName = $res->{metadata}{songName};
        my $songAuthorName = $res->{metadata}{songAuthorName};
        $songDirectory = "${id} (${songName} - ${songAuthorName})";
        my $difficulties = $version->{diffs};
        foreach my $difficultyData (@{$difficulties}) {
          next unless $difficultyData->{characteristic} eq $gameMode;
          next unless $difficultyData->{difficulty} eq $difficulty;
          $noteCount = $difficultyData->{notes};
          last VERSION;
        }
      }
    }

    if ($downloadURL) {
      if ($DEBUG) {
        $downloaded++;
        next;
      };
      next if $downloaded >= 10;
      if ($downloadURL =~ m/\.zip$/) {
        $songDirectory =~ s{[<>:/\\|?*"\x00-\x1f].*$}{};

        my $songPath = catdir($SongsFolder, $songDirectory);

        $SONG_HASH_TO_PATH->{$hash} = $songPath;

        # TODO compute and check hash to see if we need to download a new version?
        if (!-f catfile($songPath, "Info.dat")) {
          my $zip = Archive::Zip->new();
          say "Downloading $downloadURL";

          my $zipData = get($downloadURL);
          open my $fh, "+<", \$zipData;
          $zip->readFromFileHandle($fh);

          say "Unpacking $downloadURL to $songPath";

          make_path $songPath;
          chdir $songPath;

          $zip->extractTree();

          $songInfo = getSongInfo($hash);

          if ($songInfo) {
            my $songPath = $songInfo->{songPath};
            BEAT_MAP_SET: foreach my $beatMapSet (@{$songInfo->{_difficultyBeatmapSets}}) {
              next unless $beatMapSet->{_beatmapCharacteristicName} eq $gameMode;
              foreach my $beatMapData (@{$beatMapSet->{_difficultyBeatmaps}}) {
                next unless $difficultyRank == $beatMapData->{_difficultyRank};
                my $beatMapPath = catfile($songPath,$beatMapData->{_beatmapFilename});
                last BEAT_MAP_SET unless -f $beatMapPath;

                my $beatmap = decode_json(read_file($beatMapPath));
                my $notes = $beatmap->{_notes};
                $noteCount = $#{$notes} + 1;
                my $secondsPerBeat = 60 / $songInfo->{_beatsPerMinute};
                $duration = $notes->[-1]{_time} * $secondsPerBeat;
                last BEAT_MAP_SET;
              }
            }
            warn "Couldn't find note count for $gameMode/$difficultyRank from $songPath" if not defined $noteCount;
          }

          $downloaded++;
        }
      } else {
        say "TODO download $downloadURL";
      }
    }

    next unless defined $noteCount;
    $data->{duration} = $duration;

    my $maxScore = (($noteCount - 13) * 8 * 115) + 5611;
    my $potentialScore = $data->{potentialScore};
    
    my $potentialAccuracy = $potentialScore / $maxScore;
    # don't pick this if it is too hard
    #next if $potentialAccuracy < 0.5;

    $data->{potentialAccuracy} = $potentialAccuracy;

    push @{$newSongs}, $data;
    push @{$NEW_SONG_DATA}, $data;
    #last if $#{$newSongs} >= 200;
  }

  say "New Songs: " . ($#{$newSongs} + 1);

  writePlaylist([sort { $b->{potentialAccuracy} <=> $a->{potentialAccuracy} } @{$newSongs}], "not-played.bplist", "Not Played");
}

sub saveSongsToImprove {
  my $songsToImprove;

  foreach my $data (sort { $b->{potentialImprovement} <=> $a->{potentialImprovement} } values %{$SONGS_TO_IMPROVE}) {
    push @{$songsToImprove}, $data;
    #last if $#{$songsToImprove} >= 100;
  }

  say "Songs To Improve: " . ($#{$songsToImprove} + 1);

  writePlaylist($songsToImprove, "to-improve.bplist", "To Improve");
}

my $LAST_PLAYED;
my $AVERAGE_SCORE;

sub saveSongPlayHistoryData {
  my ($beatsaberFolder, $merged) = @_;

  my $songPlayFile = catfile($beatsaberFolder, "UserData", "SongPlayData.json");

  return unless -f $songPlayFile;

  open my $fh, ">", $songPlayFile;
  $fh->print(encodeJson $merged);
  close $fh;
}

sub loadSongPlayHistoryData2 {
  my ($beatsaber_folder, $merged) = @_;

  my $song_play_file = catfile($beatsaber_folder, "UserData", "SongPlayData.json");

  return unless -f $song_play_file;

  say "Reading scores from $song_play_file at ", ts();

  my $song_play_data = decode_json(read_file($song_play_file));

  foreach my $key (keys %{$song_play_data}) {
    my $data = $song_play_data->{$key};
    foreach my $play (@{$data}) {
      # don't mutate Date into a string.
      my $date = $play->{"Date"};
      $merged->{$key}{$date} = $play;
    }
  }
}

sub loadSongPlayHistoryData {
  my ($merged) = {};

  loadSongPlayHistoryData2($BeatSaberFolder, $merged);

  my $victims;

  opendir(my $dh, $BeatSaberFolders);
  while (my $file = readdir $dh) {
    next if $file =~ m/^\./;
    my $path = catdir($BeatSaberFolders, $file, 'Beat Saber');
    push @$victims, $path;
    loadSongPlayHistoryData2($path, $merged);
  }
  closedir $dh;

  foreach my $key (keys %{$merged}) {
    my $data = $merged->{$key};
    my $temp;
    foreach my $date (sort keys %{$data}) {
      push @{$temp}, $data->{$date};
    }
    $merged->{$key} = $temp;
  }

  saveSongPlayHistoryData($BeatSaberFolder, $merged);

  my $difficulties = [qw(
    Easy
    Normal
    Hard
    Expert
    ExpertPlus
  )];

  foreach my $key (keys %{$merged}) {
    next unless ($key =~ m/^(\S+)___(\d)___(\S+)$/);
    my ($levelId, $difficulty, $gameMode) = ($1, $2, $3);

    $difficulty = $difficulties->[$difficulty];

    my $plays = $merged->{$key};

    my $lastPlayed = $LAST_PLAYED->{$levelId} // 0;
    my $totalWeight = 0;
    my $totalScore = 0;

    foreach my $play (@{$plays}) {
      next unless $play->{LastNote} == -1;
      my $timestamp = $play->{Date}/1000.0;
      $lastPlayed = $timestamp if ($timestamp > $lastPlayed);
      my $age = ($NOW - $timestamp) / $SECONDS_PER_DAY;
      my $weight = 1 / $age;
      $totalScore += $play->{RawScore} * $weight;
      $totalWeight += $weight;
    }

    if ($totalWeight > 0) {
      my $averageScore = $totalScore / $totalWeight;
      $AVERAGE_SCORE->{$levelId}{$difficulty}{$gameMode} = $averageScore;
    }

    $LAST_PLAYED->{$levelId} = $lastPlayed;
  }

  foreach my $levelId ($LAST_PLAYED) {
    my $lastPlayed = $LAST_PLAYED->{$levelId};
    my $songId = $levelId;
    while (exists $SameSongs->{$songId}) {
      $songId = $SameSongs->{$songId};
      $LAST_PLAYED->{$levelId} = $lastPlayed if $lastPlayed > $LAST_PLAYED->{$levelId};
    }
  }
}

my $targetDuration = 2.5 * 60;

sub durationWeight {
  my ($duration) = @_;

  my $weight = 1 - (abs($duration - $targetDuration) / $targetDuration);
  return 0.01 if $weight < 0;
  return $weight;
}

sub ageWeight {
  my ($levelId) = @_;
  my $lastPlayed = $LAST_PLAYED->{$levelId} // 0;

  my $songId = $levelId;
  while (exists $SameSongs->{$songId}) {
    $songId = $SameSongs->{$songId};
    my $temp = $LAST_PLAYED->{$songId} // 0;
    $lastPlayed = $temp if $temp > $lastPlayed;
  }

  my $age = ($NOW - $lastPlayed) / $SECONDS_PER_DAY;
  $age = 14 if ($age > 14);
  $age = 0 if ($age < 1);

  return ($age / 14) ** 2;
}

sub saveWorkout {
  foreach my $song (@{$NEW_SONG_DATA}) {
    my $hash = uc $song->{songHash};
    my $levelId = "custom_level_" . $hash;
    $song->{levelId} = $levelId;

    my $potentialAccuracy = $song->{potentialAccuracy};

    my $potentialWeight = (1 - abs($potentialAccuracy - 0.7)) ** 2;

    my $likeCount = 0;

    $likeCount++ if exists $FAVOURITES->{$levelId};

    my $likeWeight = 1 + $likeCount;

    $song->{weight} = $potentialWeight * $likeWeight * ageWeight($levelId) * durationWeight($song->{duration});
  }

  my $workout;
  my $workoutDuration;
  my $selectedLevels;

  do {
    my $victim = (sort {$b->{weight} <=> $a->{weight}} @{$NEW_SONG_DATA})[0];

    push @{$workout}, $victim;
    $workoutDuration = $victim->{duration};
    my $songId = $victim->{levelId};
    $selectedLevels->{$songId} = 1;
    while (exists $SameSongs->{$songId}) {
      $songId = $SameSongs->{$songId};
      $selectedLevels->{$songId} = 1;
    }
  };


  my $totalWeight = 0;

  foreach my $data (values %{$OLD_SONGS}) {
    my $hash = uc $data->{songHash};
    my $levelId = "custom_level_" . $hash;
    $data->{levelId} = $levelId;

    my $gameMode = $data->{difficulty}{gameMode};
    next unless $gameMode =~ s/^Solo//;
    my $difficultyRank = $data->{difficulty}{difficulty};
    my $difficulty = $difficultyMap->{$difficultyRank};

    my $noteCount;

    my $songInfo = getSongInfo($hash);
    my $duration;

    if ($songInfo) {
      my $songPath = $songInfo->{songPath};
      BEAT_MAP_SET: foreach my $beatMapSet (@{$songInfo->{_difficultyBeatmapSets}}) {
        next unless $beatMapSet->{_beatmapCharacteristicName} eq $gameMode;
        foreach my $beatMapData (@{$beatMapSet->{_difficultyBeatmaps}}) {
          next unless $difficultyRank == $beatMapData->{_difficultyRank};
          my $beatMapPath = catfile($songPath,$beatMapData->{_beatmapFilename});
          last BEAT_MAP_SET unless -f $beatMapPath;

          my $beatmap = decode_json(read_file($beatMapPath));
          my $notes = $beatmap->{_notes};
          $noteCount = $#{$notes} + 1;
          my $secondsPerBeat = 60 / $songInfo->{_beatsPerMinute};
          $duration = $notes->[-1]{_time} * $secondsPerBeat;
          last BEAT_MAP_SET;
        }
      }
      warn "Couldn't find note count for $gameMode/$difficultyRank from $songPath" if not defined $noteCount;
    }

    next if not defined $noteCount;

    $data->{duration} = $duration;

    my $maxScore = (($noteCount - 13) * 8 * 115) + 5611;
    my $potentialScore = $data->{potentialScore};
    
    my $potentialAccuracy = $potentialScore / $maxScore;

    $data->{potentialAccuracy} = $potentialAccuracy;

    my $potentialWeight = (1 - abs($potentialAccuracy - 0.7)) ** 2;

    # TODO get votes?
    my $likeCount = 0;

    $likeCount++ if exists $FAVOURITES->{$levelId};

    my $likeWeight = ($likeCount + 1);

    my $potentialImprovement = $data->{potentialImprovement};
    my $improvementWeight = 1;
    if ($potentialImprovement) {
      $improvementWeight = $potentialImprovement + 1;
    }

    my $averageScore = $AVERAGE_SCORE->{$levelId}{$difficulty}{$gameMode};
    my $averageAccuracy = $averageScore / $maxScore;
    my $averageAccuracyWeight = (1 - abs($averageAccuracy - 0.7)) ** 2;
    
    my $weight = $averageAccuracyWeight * $potentialWeight * $improvementWeight * $likeWeight * ageWeight($levelId) * durationWeight($duration);
    
    $data->{weight} = $weight;
    $totalWeight += $weight;
  }

  # TODO what about songs for which we have no predicted score?

  # pick 40 minutes of songs.
  my $targetWorkoutDuration = 40 * 60;

  foreach my $victim (sort {$b->{weight} <=> $a->{weight}} grep {defined $_->{weight}} values %{$OLD_SONGS}) {
    my $levelId = $victim->{levelId};
    next if exists $selectedLevels->{$levelId};

    push @{$workout}, $victim;
    $workoutDuration += $victim->{duration};

    last if $workoutDuration >= $targetWorkoutDuration;

    my $songId = $victim->{levelId};
    $selectedLevels->{$songId} = 1;
    while (exists $SameSongs->{$songId}) {
      $songId = $SameSongs->{$songId};
      $selectedLevels->{$songId} = 1;
    }
  }

  @{$workout} = sort {$b->{potentialAccuracy} <=> $a->{potentialAccuracy}} @{$workout};

  push @{$workout}, shift @{$workout};

  writePlaylist($workout, "workout.bplist", "Workout");
}

##############################################################

make_path $DataDir;

make_path $CacheDir;

open STDOUT, ">", catfile($DataDir, "buildBeatSaberPlaylist2.txt");
open STDERR, ">&STDOUT";
STDOUT->autoflush(1);

say "Starting run at ", ts();

loadFavourites();

fetchScores();
fetchLeaderboards();
fetchNeighbours();

loadSongPlayHistoryData();

saveNewSongs();
saveSongsToImprove();

saveWorkout();

say "Run complete at ", ts();
