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

use autodie qw(:all);

my $PlayerID = '76561198001262880';

my $DataDir = catdir(abs_path(__FILE__), updir(), "data");

my $CacheDir = catdir($DataDir, "cache");

my $BeatSaberFolder = "G:\\Steam\\steamapps\\common\\Beat Saber";

my $PlaylistFolder = catdir($BeatSaberFolder, "Playlists");

my $SongsFolder = catdir($BeatSaberFolder, "Beat Saber_Data", "CustomLevels");

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
#  NoUpdate => $SECONDS_IN_15_MINUTES,
  NoUpdate => $SECONDS_PER_DAY, # for development
  ApproveContent => sub { return $_[0]->is_success }
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

sub get {
  my ($url) = @_;

  my $res = $ua->get($url);

  croak $res->as_string unless $res->is_success;

  return $res->content;
}

our $MY_SCORES;
our $LEADERBOARDS;
my $ranks;

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

      $MY_SCORES->{$leaderboardId} = $score;
      $LEADERBOARDS->{$leaderboardId} = $leaderboard;

      $ranks->{$leaderboardId} = $score->{rank};
    }

    $page++;
    last if $page > $pages;

    $res = decode_json(get($baseUrl . "&page=$page"));
  }
}

our $NEIGHBOURS;
our $NEIGHBOUR_SCORE_DATE;

sub fetchLeaderboards {
  my $itemsPerPage = 12;

  my $checkItemsPerPage = 1;

  foreach my $leaderboard (values %{$LEADERBOARDS}) {
    my $id = $leaderboard->{id};
    my $rank = $ranks->{$id};
    my $myScore = $MY_SCORES->{$id}{baseScore};
    say "Considering leaderboard ${id} where we have rank #${rank} with score ${myScore}";

    my $page = ceil($rank / $itemsPerPage);

    my $res = decode_json(get("https://scoresaber.com/api/leaderboard/by-id/${id}/scores?page=${page}"));

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

    foreach my $rec (@{$scores}) {
      my $playerId = $rec->{leaderboardPlayerInfo}{id};
      # this is us, we're not our own neighbour.
      next if $playerId == $PlayerID;
      my $score = $rec->{baseScore};
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
    }
  }
}

my $NEW_SONGS;
my $NEW_SONGS_WEIGHT;
my $SONGS_TO_IMPROVE;
my $NEIGHBOUR_SCORE;
my $NEW_SONGS_NEIGHBOUR_SCORE;

sub fetchNeighbours {
  foreach my $neighbour (sort { $NEIGHBOURS->{$b} <=> $NEIGHBOURS->{$a} } keys %{$NEIGHBOURS}) {
    my $neighbourCount = $NEIGHBOURS->{$neighbour};
    # pick only those people that appear frequently.
    last if $neighbourCount <= 2;
    say "Considering player $neighbour who we share $neighbourCount leaderboards with";

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
        $NEIGHBOUR_SCORE->{$leaderboardId}{total} += $delta * $score->{baseScore} * $scoreAdjust;
        $NEIGHBOUR_SCORE->{$leaderboardId}{totalWeight} += $delta;
      } else {
        $NEW_SONGS->{$leaderboardId} = $leaderboard;
        $NEW_SONGS_WEIGHT->{$leaderboardId} += $delta;
        $NEW_SONGS_NEIGHBOUR_SCORE->{$leaderboardId}{total} += $delta * $score->{baseScore} * $scoreAdjust;
        $NEW_SONGS_NEIGHBOUR_SCORE->{$leaderboardId}{totalWeight} += $delta;
      }
    }
  }

  foreach my $leaderboardId (keys %{$NEIGHBOUR_SCORE}) {
    my $scores = $NEIGHBOUR_SCORE->{$leaderboardId};
    my $totalWeight = $scores->{totalWeight};
    my $averageBaseScore = 0;
    if ($totalWeight > 0) {
      $averageBaseScore = floor($scores->{total} / $totalWeight);
    }
    my $myBaseScore = $MY_SCORES->{$leaderboardId}{baseScore};
    if ($averageBaseScore > $myBaseScore) {
      my $data = $LEADERBOARDS->{$leaderboardId};
      $data->{potentialScore} = $averageBaseScore;
      $data->{potentialImprovement} = ($averageBaseScore - $myBaseScore) / $myBaseScore;
      $SONGS_TO_IMPROVE->{$leaderboardId} = $data;
    }
    $NEIGHBOUR_SCORE->{$leaderboardId} = $averageBaseScore;
  }

  foreach my $leaderboardId (keys %{$NEW_SONGS_NEIGHBOUR_SCORE}) {
    my $scores = $NEW_SONGS_NEIGHBOUR_SCORE->{$leaderboardId};
    my $totalWeight = $scores->{totalWeight};
    my $averageBaseScore = 0;
    if ($totalWeight > 0) {
      $averageBaseScore = floor($scores->{total} / $totalWeight);
    }
    my $data = $NEW_SONGS->{$leaderboardId};
    $data->{potentialScore} = $averageBaseScore;
    $NEW_SONGS_NEIGHBOUR_SCORE->{$leaderboardId} = $averageBaseScore;
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
  my ($songData, $folderName, $file_name, $title) = @_;

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

  my $folderPath = catdir($PlaylistFolder, $folderName);

  make_path($folderPath);

  open my $fh, ">", catfile($folderPath, $file_name);
  $fh->print(encodeJson $playlist);
  close $fh;
}

sub saveNewSongs {
  my $songHashFile = catfile($BeatSaberFolder, "UserData", "SongCore", "SongHashData.dat");
  my $songHashToPath;

  if (-f $songHashFile) {
    my $songHashData = decode_json(read_file($songHashFile));
    foreach my $dir (keys %{$songHashData}) {
      $songHashToPath->{uc $songHashData->{$dir}{songHash}} = $dir;
    }
  }

  my $songInfos;

  my $newSongs;

  foreach my $leaderboardId (sort { $NEW_SONGS_WEIGHT->{$b} <=> $NEW_SONGS_WEIGHT->{$a} } keys %{$NEW_SONGS_WEIGHT}) {
    my $data = $NEW_SONGS->{$leaderboardId};
    my $hash = uc $data->{songHash};

    my $gameMode = $data->{difficulty}{gameMode};
    next unless $gameMode =~ s/^Solo//;
    my $difficultyRank = $data->{difficulty}{difficulty};
    my $difficulty = $difficultyMap->{$difficultyRank};

    my $noteCount;
    my $downloadURL;
    my $songPath = $songHashToPath->{$hash};
    my $songDirectory;

    my $songInfo;
    if (exists $songInfos->{$hash}) {
      $songInfo = $songInfos->{$hash};
    } else {
      if ($songPath && -d $songPath) {
        my $songInfoPath = catfile($songPath, "Info.dat");
        if (-f $songInfoPath) {
          $songInfo = decode_json(read_file($songInfoPath));
          $songInfos->{$hash} = $songInfo;
        }
      }
    }

    if ($songInfo) {
      BEAT_MAP_SET: foreach my $beatMapSet (@{$songInfo->{_difficultyBeatmapSets}}) {
        next unless $beatMapSet->{_beatmapCharacteristicName} eq $gameMode;
        foreach my $beatMapData (@{$beatMapSet->{_difficultyBeatmaps}}) {
          next unless $difficultyRank == $beatMapData->{_difficultyRank};
          my $beatMapPath = catfile($songPath,$beatMapData->{_beatmapFilename});
          last BEAT_MAP_SET unless -f $beatMapPath;

          my $beatmap = decode_json(read_file($beatMapPath));
          my $notes = $beatmap->{_notes};
          $noteCount = $#{$notes} + 1;
          last BEAT_MAP_SET;
        }
      }
      warn "Couldn't find note count for $gameMode/$difficultyRank from $songPath" if not defined $noteCount;
    }

    if (not defined $noteCount) {
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
      if ($downloadURL =~ m/\.zip$/) {
        $songDirectory =~ s{[<>:/\\|?*"\x00-\x1f].*$}{};

        $songPath = catdir($SongsFolder, $songDirectory);

        $songHashToPath->{$hash} = $songPath;

        # TODO compute and check hash
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
        }
      } else {
        say "TODO download $downloadURL";
      }
    }

    next unless defined $noteCount;

    my $maxScore = (($noteCount - 13) * 8 * 115) + 5611;
    my $potentialScore = $data->{potentialScore};
    
    my $potentialAccuracy = $potentialScore / $maxScore;
    # don't pick this if it is too hard
    next if $potentialAccuracy < 0.6;

    $data->{potentialAccuracy} = $potentialAccuracy;

    push @{$newSongs}, $data;
    last if $#{$newSongs} >= 100;
  }

  my $ts = ts2();

  writePlaylist([sort { $b->{potentialAccuracy} <=> $a->{potentialAccuracy} } @{$newSongs}], "not-played", "${ts}.bplist", "Not Played ${ts}");
}

sub saveSongsToImprove {
  my $songsToImprove;

  foreach my $data (sort { $b->{potentialImprovement} <=> $a->{potentialImprovement} } values %{$SONGS_TO_IMPROVE}) {
    push @{$songsToImprove}, $data;
    last if $#{$songsToImprove} >= 100;
  }

  my $ts = ts2();

  writePlaylist($songsToImprove, "to-improve", "${ts}.bplist", "To Improve ${ts}");
}

##############################################################

make_path $DataDir;

make_path $CacheDir;

open STDOUT, ">", catfile($DataDir, "buildBeatSaberPlaylist2.txt");
open STDERR, ">&STDOUT";
STDOUT->autoflush(1);

say "Starting run at ", ts();

fetchScores();
fetchLeaderboards();
fetchNeighbours();

saveNewSongs();
saveSongsToImprove();

say "Songs To Improve: " . scalar keys %{$SONGS_TO_IMPROVE};
say "New Songs: " . scalar keys %{$NEW_SONGS};

say "Run complete at ", ts();
