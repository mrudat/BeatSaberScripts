#!perl

use warnings;
use strict;

use v5.16;

use JSON qw(decode_json);
use File::Find;
use POSIX qw(strftime);
use File::Spec::Functions qw(catdir catfile updir rel2abs);
use File::stat;
use Storable;
use Statistics::Regression;
use Cwd qw(abs_path);
use File::Slurp;
use File::Path qw(make_path);

use autodie qw(:all);

my $DEFAULT_NOTE_JUMP_SPEED = 10;

my $DataDir = catdir(abs_path(__FILE__), updir(), "data");

my $CacheDir = catdir($DataDir, "cache");

my $BeatSaberFolder = "G:\\Steam\\steamapps\\common\\Beat Saber";

my $PlayListFolder = catdir($BeatSaberFolder, "Playlists");

my $BeatSaviorDataFolder = catdir($ENV{appdata},"Beat Savior Data");

my $BeatSaberAppdataFolder = catdir($ENV{localappdata},updir(),"LocalLow","Hyperboloc Magnetism","Beat Saber");

my $SecondsPerDay = 60 * 60 * 24.0;

my $WorkoutDuration = 40 * 60;

# TODO actually parse the file format
# from https://docs.google.com/spreadsheets/d/13wyoviJAplYOrsMocOA7YNXJxVRHd74G7z4U2jhCZa4
my $VanillaMapBPM = {
  "\$100Bills" => 201,
  "BalearicPumping" => 111,
  "BeatSaber" => 166,
  "Breezer" => 112.5,
  "CommercialPumping" => 105,
  "CountryRounds" => 210,
  "Escape" => 175,
  "Legend" => 95,
  "LvlInsane" => 160,
  "TurnMeOn" => 80,

  "BeThereForYou" => 126,
  "Elixia" => 128,
  "INeedYou" => 128,
  "RumNBass" => 132,
  "UnlimitedPower" => 198,

  "Origins" => 175,
  "ReasonForLiving" => 128,
  "GiveALittleLove" => 128,
  "FullCharge" => 125,
  "Immortal" => 150,
  "BurningSands" => 142,

  "CrabRave" => 125,
  "PopStars" => 170,
  "OneHope" => 145,
  "AngelVoices" => 166,
  "FitBeat" => 215,

  "Crystallized" => 174,
  "CycleHit" => 175,
  "WhatTheCat" => 200,
  "ExitThisEarthsAtomosphere" => 170,
  "Ghost" => 220,
  "LightItUp" => 174,

  "IntoTheDream" => 183,
  "ItTakesMe" => 220,
  "LudicrousPlus" => 260,
  "SpinEternally" => 125,

  "Boundless" => 170,
  "EmojiVIP" => 163,
  "Epic" => 128,
  "FeelingStronger" => 175,
  "Overkill" => 174,
  "Rattlesnake" => 175,
  "Stronger" => 170,
  "ThisTime" => 160,
  "TillItsOver" => 110,
  "WeWontBeAlone" => 175,

  "BadLiar" => 89,
  "Believer" => 125,
  "Digital" => 167,
  "ItsTime" => 105,
  "Machine" => 81,
  "Natural" => 100,
  "Radioactive" => 136.6,
  "Thunder" => 168,
  "Warriors" => 156,
  "WhateverItTakes" => 135,
};

# TODO actually parse the file format?
my ($RenameLevels) = {
  '$100Bills' => "100Bills",
};

my ($SameSongs) = {
  "custom_level_B68BF61AC6BE0E128BE32A85810D42E7C53F4756" => "BeatSaber",
  "custom_level_2C002D2874E029DB43F3C7CF9BB271AE0D769B74" => "custom_level_45F9480A43DDEA9FF338BF449AD9EAD62F73EB52",
  "custom_level_2C002D2874E029DB43F3C7CF9BB271AE0D769B74" => "custom_level_E6AF862558EF500F16D3B82161BBDFB1D2C296BF",
};

my $Now = time();

my ($IgnoredGameModes) = {
  "Lightshow" => 1,
  "360Degree" => 1,
  "OneSaber" => 1,
};

do {
  my $json = JSON->new->pretty->canonical;

  sub encodeJson { return $json->encode(@_); }
};

sub ts {
  return strftime("%Y-%m-%dT%H:%M:%S",localtime());
}

sub isIgnoredGameMode {
  my ($game_mode) = @_;
  return exists $IgnoredGameModes->{$game_mode};
}

sub isBeatmapTooShort {
  my ($beatmap) = @_;

  my $notes = $beatmap->{_notes};

  return 1 unless defined $notes;

  return 1 unless ($#{$notes} +1) > 15;

  my $duration = $notes->[-1]{_time};

  my $beats_per_minute = $beatmap->{beats_per_minute};

  return 1 unless defined $beats_per_minute;

  my $seconds_per_beat = 60 / $beats_per_minute;

  $beatmap->{seconds_per_beat} = $seconds_per_beat;

  $duration = $duration * $seconds_per_beat;

  # TODO use SongDurationCache?
  $beatmap->{duration} = $duration;

  return 1 unless $duration > 60;

  return 0;
}

sub loadOSTOrDLCBeatMap {
  my ($file_path, $cache_path, $beatmaps) = @_;

  my $cache_stat = stat($cache_path);
  my $file_stat = stat($file_path);

  if (defined $cache_stat && -f $cache_stat && $cache_stat->mtime >= $file_stat->mtime) {
    #say "Loading cached beatmaps from $file_path at ", ts();
    my $cache = retrieve($cache_path);
    foreach my $level_id (keys %$cache) {
      foreach my $mode (keys %{$cache->{$level_id}}) {
        foreach my $difficulty (keys %{$cache->{$level_id}{$mode}}) {
          $beatmaps->{$level_id}{$mode}{$difficulty} = $cache->{$level_id}{$mode}{$difficulty};
        }
      }
    }
  } else {
    say "Parsing beatmaps from $file_path at ", ts();
    my $cache;
    do {
      open my $in, "<:raw", $file_path;

      local $/ = "\0";

      my ($level_id);

      while(my $line = <$in>) {
        chomp $line;
        $level_id = $1 if $line =~ m/^(.+)BeatmapData/;
        next unless defined $level_id;
        my $temp = $line;
        next unless substr($line,0,2) eq '{"';

        # discard everything after final }, because we sometimes have more stuff
        # TODO actually parse the file format?
        my $lastbracket = rindex $line, '}';

        next unless $lastbracket > 0;

        $line = substr $line,0,$lastbracket + 1;

        my $difficulty = "Normal";
        $difficulty = $1 if ($level_id =~ s/(Easy|Normal|Hard|Expert|ExpertPlus)$//);

        my $game_mode = "Standard";
        $game_mode = $1 if ($level_id =~ s/(NoArrows|OneSaber|90Degree|360Degree)//);

        if (isIgnoredGameMode($game_mode)){
          undef $level_id;
          next;
        }

        #say "$level_id $game_mode $difficulty";

        my $beatmap;

        eval {
          $beatmap = decode_json($line);
        };

        if ($@) {
          say $temp;
          warn $@;
          next;
        }

        unless (exists $VanillaMapBPM->{$level_id}) {
          say "dont know BPM for ", $level_id;
          undef $level_id;
          next;
        }

        $beatmap->{beats_per_minute} = $VanillaMapBPM->{$level_id};

        if (isBeatmapTooShort($beatmap)){
          undef $level_id;
          next;
        }

        # TODO if we actually parse the file, we wouldn't need to do this.
        while (exists $RenameLevels->{$level_id}) {
          $level_id = $RenameLevels->{$level_id};
        }

        # TODO read the actual value
        $beatmap->{note_jump_speed} = 0;

        delete $beatmaps->{_events};

        # TODO perhaps we should care about obstacles?
        delete $beatmaps->{_obstacles};

        $beatmaps->{$level_id}{$game_mode}{$difficulty} = $beatmap;
        $cache->{$level_id}{$game_mode}{$difficulty} = $beatmap;

        undef $level_id;
      }

      close $in;
    };
    store $cache, $cache_path;
  }
}

sub loadCustomBeatMaps {
  my ($beatmaps) = @_;

  my $song_hash_file = catfile($BeatSaberFolder, "UserData", "SongCore", "SongHashData.dat");

  die "$song_hash_file not found, is SongCore installed?" unless -f $song_hash_file;

  my $song_hash_data = decode_json(read_file($song_hash_file));

  foreach my $song_dir (keys %{$song_hash_data}) {
    next unless -d $song_dir;

    my $song_info_path = catfile($song_dir, "Info.dat");

    next unless -f $song_info_path;

    my $songHash = $song_hash_data->{$song_dir}{songHash};
    my $level_id = "custom_level_$songHash";

    my $song_data = decode_json(read_file($song_info_path));

    foreach my $game_mode_data (@{$song_data->{_difficultyBeatmapSets}}) {
      my $game_mode = $game_mode_data->{_beatmapCharacteristicName};
      next if isIgnoredGameMode($game_mode);
      foreach my $difficulty_data (@{$game_mode_data->{_difficultyBeatmaps}}) {
        my $difficulty_file = catfile($song_dir,$difficulty_data->{_beatmapFilename});
        next unless -f $difficulty_file;

        my $difficulty = $difficulty_data->{_difficulty};

        my $beatmap = decode_json(read_file($difficulty_file));

        $beatmap->{beats_per_minute} = $song_data->{_beatsPerMinute};

        next if isBeatmapTooShort($beatmap);

        $beatmap->{note_jump_speed} = $difficulty_data->{_noteJumpMovementSpeed};

        $beatmap->{songName} = $song_data->{_songName};
        $beatmap->{songAuthorName} = $song_data->{_songAuthorName},
        $beatmap->{levelAuthorName} = $song_data->{_levelAuthorName};

        delete $beatmaps->{_events};

        # TODO perhaps we should care about obstacles?
        delete $beatmaps->{_obstacles};

        $beatmaps->{$level_id}{$game_mode}{$difficulty} = $beatmap;
      }
    }
  }
}

sub loadDLCBeatMaps {
  my ($beatmaps) = @_;

  find(
    sub {
      return unless -f $_;
      my $dlc_file = $_;

      loadOSTOrDLCBeatMap(
        $File::Find::name,
        catfile($CacheDir, "DLC-$dlc_file-BeatMaps.storable"),
        $beatmaps
      );
    },
    catdir($BeatSaberFolder, "DLC", "Levels")
  );
}

sub loadVanillaBeatMaps {
  my ($beatmaps) = @_;

  loadOSTOrDLCBeatMap(
    catfile($BeatSaberFolder, "Beat Saber_Data", "sharedassets0.assets"),
    catfile($CacheDir, "OST-BeatMaps.storable"),
    $beatmaps
  );
}

sub loadAllBeatMaps {
  my ($beatmaps) = {};

  say "Loading vanilla beatmaps at ", ts();

  loadVanillaBeatMaps($beatmaps);

  say "Loading DLC beatmaps at ", ts();

  loadDLCBeatMaps($beatmaps);

  say "Loading custom beatmaps at ", ts();

  loadCustomBeatMaps($beatmaps);

  say "Beatmaps loaded at ", ts();

  return $beatmaps;
}

my $difficulties = [qw(
  Easy
  Normal
  Hard
  Expert
  ExpertPlus
)];

sub loadSongPlayHistoryData {
  my ($beatmaps) = @_;

  my $song_play_file = catfile($BeatSaberFolder, "UserData", "SongPlayData.json");

  return unless -f $song_play_file;

  say "Reading scores from $song_play_file at ", ts();

  my $song_play_data = decode_json(read_file($song_play_file));

  foreach my $key (keys %{$song_play_data}) {
    next unless ($key =~ m/^(\S+)___(\d)___(\S+)$/);
    my ($level_id, $difficulty, $game_mode) = ($1, $2, $3) ;

    next if isIgnoredGameMode($game_mode);

    $difficulty = $difficulties->[$difficulty];

    next unless exists $beatmaps->{$level_id}{$game_mode}{$difficulty};

    my $beatmap = $beatmaps->{$level_id}{$game_mode}{$difficulty};

    next if isBeatmapTooShort($beatmap);

    my $notes = $beatmap->{_notes};

    my $noteCount = $#{$notes} + 1;

    # from https://www.reddit.com/r/beatsaber/comments/kswak5/how_to_calculate_the_highest_possible_score_of/
    my $maxScore = (($noteCount - 13) * 8 * 115) + 5611;

    my $scoreFactor = 1.0 / $maxScore;

    my $plays = $song_play_data->{$key};

    my $last_played = 0;

    foreach my $play (@{$plays}) {
      next unless $play->{LastNote} == -1;
      $play->{score} = $play->{RawScore} * $scoreFactor;
      my $timestamp = $play->{Date}/1000.0;
      $last_played = $timestamp if ($timestamp > $last_played);
      my $date = strftime('%Y-%m-%d',localtime($timestamp));
      push @{$beatmap->{song_play_data}{$date}}, $play;
    }

    $beatmap->{last_played} = $last_played;
  }
}

my $regressionModel = [qw(n^2 n t^2 t c)];

sub recordBloqHit {
  my (
    $bloq_stats,
    $saber,
    $position,
    $direction,
    $last_position,
    $last_direction,
    $time_since_last_hit,
    $note_jump_speed,
    $score,
    $timestamp
  ) = @_;

  $time_since_last_hit = 2 if $time_since_last_hit > 2;

  my $t = [
    $note_jump_speed**2,
    $note_jump_speed,
    $time_since_last_hit**2,
    $time_since_last_hit,
    1.0
  ];

  my (@directions);

  my $age = ($Now - $timestamp) / $SecondsPerDay;

  my $weight = 1.0 / $age;

  # TODO is it better to track each one separately?
  $score = $score->[0] + $score->[1] + $score->[2];

  push @directions, $direction;

  # 9 is the dot bloq; we use it later for if a particular saber/position/direction hasn't been hit before.
  push @directions, 9 if $direction != 9;

  my @data;

  if ($time_since_last_hit > 1) {
    my $datum = $bloq_stats->{opening}[$saber][$position][$direction];

    if (!defined $datum) {
      $datum = {
        minTime => 3600,
        maxTime => 0,
        stats => Statistics::Regression->new("$saber $position-$direction", $regressionModel)
      };

      push @{$bloq_stats->{stats}}, $datum;

      $bloq_stats->{opening}[$saber][$position][$direction] = $datum;
    }

    push @data, $datum;
  }

  if (defined $last_position) {
    my $datum = $bloq_stats->{pattern}[$saber][$last_position][$last_direction][$position][$direction];

    if (!defined $datum) {
      $datum = {
        minTime => 3600,
        maxTime => 0,
        stats => Statistics::Regression->new("$saber $last_position-$last_direction -> $position-$direction", $regressionModel)
      };

      push @{$bloq_stats->{stats}}, $datum;

      $bloq_stats->{pattern}[$saber][$last_position][$last_direction][$position][$direction] = $datum;
    }

    push @data, $datum;
  }

  foreach my $data (@data) {
    foreach $direction (@directions) {
      $data->{minTime} = $time_since_last_hit if $time_since_last_hit < $data->{minTime};
      $data->{maxTime} = $time_since_last_hit if $time_since_last_hit > $data->{maxTime};

      $data->{stats}->include($score, $t, $weight);
    }
  }
}

sub recordBloqHits {
  my ($bloq_stats, $hits, $note_jump_speed, $play_timestamp) = @_;

  my $last_hit_times;
  my $last_positions;
  my $last_directions;

  foreach my $hit (@{$hits}) {
    my $saber = $hit->{noteType};
    my $hit_time = $hit->{time};
    #my $id = $hit->{id};

    my $last_hit_time = $last_hit_times->[$saber] || 0;
    my $last_direction = $last_directions->[$saber];
    my $last_position = $last_positions->[$saber];
    my $time_since_last_hit = $hit_time - $last_hit_time;

    next if $time_since_last_hit < 0; # <0 wtf?
    $time_since_last_hit = 2 if $time_since_last_hit > 2;
    my $direction = $hit->{noteDirection};
    my $position = $hit->{index};

    my $absolute_hit_time = $hit_time + $play_timestamp;

    my $score = $hit->{score};

    recordBloqHit(
      $bloq_stats,
      $saber,
      $position,
      $direction,
      $last_position,
      $last_direction,
      $time_since_last_hit,
      $note_jump_speed,
      $score,
      $absolute_hit_time
    );

    $last_hit_times->[$saber] = $hit_time;
    $last_directions->[$saber] = $direction;
    $last_positions->[$saber] = $position;
  }
}

sub loadBeatSaviourData {
  my ($beatmaps, $bloq_stats) = @_;

  find(
    sub {
      return unless $_ =~ m/^(\d+-\d+-\d+)\.bsd$/;
      my ($date) = $1;
      my $bsd_file = $_;

      my ($timestamp) = stat($bsd_file)->mtime;

      say "Reading from $bsd_file...";

      open my $fh, "<", $bsd_file;
      my $header = decode_json <$fh>;
      my $player_id = $header->{playerID};
      foreach my $line (<$fh>) {
        my $song_data = decode_json $line;
        next if $song_data->{playerID} != $player_id;
        next if $song_data->{songStartTime} != 0.0;
        next if $song_data->{songSpeed} != 1.0;

        my $song_hash = $song_data->{songID};
        my $level_id;
        if ($song_hash =~ m/^[A-Za-z0-9]{40}$/) {
          $level_id = "custom_level_${song_hash}";
        } else {
          $level_id = $song_hash;
          undef $song_hash;
        }

        my $difficulty = ucfirst $song_data->{songDifficulty};

        my $game_mode = $song_data->{gameMode};

        my $duration = $song_data->{songDuration};

        next unless $duration > 60;

        next unless exists $beatmaps->{$level_id}{$game_mode}{$difficulty};

        my $beatmap = $beatmaps->{$level_id}{$game_mode}{$difficulty};

        $beatmap->{duration} = $duration;

        my $play_timestamp = $timestamp;

        if (exists $beatmap->{song_play_data}{$date}) {
          my $plays = $beatmap->{song_play_data}{$date};
          my $raw_score = $song_data->{trackers}{scoreTracker}{rawScore};
          foreach my $play (@{$plays}) {
            if (exists $play->{RawScore} && $play->{RawScore} == $raw_score) {
              $play_timestamp = $play->{Date} / 1000.0;
              delete $play->{RawScore};
              last;
            }
          }
        }

        if (exists $beatmap->{last_played}) {
          $beatmap->{last_played} = $play_timestamp if $play_timestamp > $beatmap->{last_played};
        } else {
          $beatmap->{last_played} = $play_timestamp;
        }

        $song_data->{timestamp} = $play_timestamp;

        my $note_jump_speed = $beatmap->{note_jump_speed} || $DEFAULT_NOTE_JUMP_SPEED;

        $note_jump_speed = $DEFAULT_NOTE_JUMP_SPEED if $note_jump_speed == 0;

        recordBloqHits(
          $bloq_stats,
          $song_data->{deepTrackers}{noteTracker}{notes},
          $note_jump_speed,
          $play_timestamp
        );

        $beatmap->{songName} ||= $song_data->{songName};
        $beatmap->{songAuthorName} ||= $song_data->{songArtist},
        $beatmap->{levelAuthorName} ||= $song_data->{songMapper};

        delete $song_data->{deepTrackers};
        delete $song_data->{trackers}{hitTracker};
        #delete $song_data->{trackers}{accuracyTracker};
        #delete $song_data->{trackers}{scoreTracker};
        delete $song_data->{trackers}{winTracker};
        #delete $song_data->{trackers}{distanceTracker};
        delete $song_data->{trackers}{scoreGraphTracker};

        push @{$beatmap->{beatSaviourData}}, $song_data;
      }
    },
    $BeatSaviorDataFolder
  );
}

sub calculateBloqStats {
  my ($bloq_stats) = @_;

  foreach my $hit_stats (@{$bloq_stats->{stats}}) {
    my $stats = $hit_stats->{stats};
    delete $hit_stats->{stats};
    my $minTime = $hit_stats->{minTime};
    if ($stats->n() > $stats->k()) {
      my ($a, $b, $c, $d, $e) = $stats->theta();
      my $maxTime = $hit_stats->{maxTime};
      $hit_stats->{func} = sub {
        my ($time, $note_jump_speed) = @_;
        return 0 if $time*2 < $minTime;
        return 1 if $time < $minTime;
        $time = $maxTime if $time > $maxTime;
        return ($a * $note_jump_speed**2)
              + ($b * $note_jump_speed)
              + ($c * $time**2)
              + ($d * $time)
              +  $e;
      };
      eval { $stats->print(); };
    } else {
      my $average = $stats->ybar();
      $hit_stats->{func} = sub {
        my ($time) = @_;
        return 0 if $time*2 < $minTime;
        return 1 if $time < $minTime;
        return $average;
      };
    }
  }
}

sub calculateBeatmapStats {
  my ($beatmaps, $bloq_stats) = @_;

  say "Calculating beatmap stats at ", ts();

  foreach my $level_id (keys %{$beatmaps}) {
    foreach my $game_mode (keys %{$beatmaps->{$level_id}}) {
      foreach my $difficulty (keys %{$beatmaps->{$level_id}{$game_mode}}) {
        my $beatmap = $beatmaps->{$level_id}{$game_mode}{$difficulty};

        my ($total_scores, $total_weight);

        if (exists $beatmap->{song_play_data}) {
          my $temp = $beatmap->{song_play_data};
          foreach my $day (keys %{$temp}) {
            foreach my $play (@{$temp->{$day}}) {
              if (exists $play->{RawScore}) {
                my $weight = $SecondsPerDay / ($Now - ($play->{Date} / 1000.0));
                $total_scores += $play->{score} * $weight;
                $total_weight += $weight;
              }
            }
          }
        }

        if (exists $beatmap->{beatSaviourData}) {
          my $beat_saviour_data = $beatmap->{beatSaviourData};

          my ($total_hit_speed, $total_hand_speed, $beatsavior_weight);

          foreach my $play (@{$beat_saviour_data}) {
            my $weight = $SecondsPerDay / ($Now - $play->{timestamp});
            $total_scores += $play->{trackers}{scoreTracker}{rawRatio} * $weight;
            $total_weight += $weight;

            my $average_hit_speed = $play->{trackers}{accuracyTracker}{averageSpeed};

            my $duration = $play->{songDuration};
            my $left_hand_speed = $play->{trackers}{distanceTracker}{leftHand} / $duration;
            my $right_hand_speed = $play->{trackers}{distanceTracker}{rightHand} / $duration;

            my $average_hand_speed = ($left_hand_speed + $right_hand_speed) / 2;

            $total_hit_speed += $average_hit_speed * $weight;
            $total_hand_speed += $average_hand_speed * $weight;
            $beatsavior_weight += $weight;
          }

          if (defined $beatsavior_weight) {
            $beatmap->{average_hit_speed} = $total_hit_speed / $beatsavior_weight;
            $beatmap->{average_hand_speed} = $total_hand_speed / $beatsavior_weight;
          }
        }

        if (defined $total_weight) {
          $beatmap->{average_score} = $total_scores / $total_weight;
        }

        my $note_jump_speed = $beatmap->{note_jump_speed} || $DEFAULT_NOTE_JUMP_SPEED;
        $note_jump_speed = $DEFAULT_NOTE_JUMP_SPEED if $note_jump_speed == 0;

        my $notes = $beatmap->{_notes};

        my $last_times;
        my $last_positions;
        my $last_directions;

        my $total_score = 0;
        my $multiplier = 1;
        my $combo = 0;
        my $note_count = $#{$notes} + 1;
        my $max_score = (($note_count - 13) * 8 * 115) + 5611;

        my $seconds_per_beat = $beatmap->{seconds_per_beat};
        next unless defined $seconds_per_beat;
        my $notes_with_stats;

        foreach my $note (@{$notes}) {
          my $saber = $note->{_type};
          my $position = $note->{_lineIndex} + 4 * $note->{_lineLayer};
          my $direction = $note->{_cutDirection};
          my $time = $note->{_time} * $seconds_per_beat;

          my $last_time = $last_times->[$saber] || 0;
          my $last_direction = $last_directions->[$saber];
          my $last_position = $last_positions->[$saber];
          my $time_since_last_note = $time - $last_time;
          my $note_score = 0;

          my $stats;
          if ($time_since_last_note < 1 && defined $last_position) {
            $stats = $bloq_stats->{pattern}[$saber][$last_position][$last_direction][$position][$direction]
                  || $bloq_stats->{pattern}[$saber][$last_position][$last_direction][$position][9] 
                  || $bloq_stats->{opening}[$saber][$position][$direction] 
                  || $bloq_stats->{opening}[$saber][$position][9]
          } else {
            $stats = $bloq_stats->{opening}[$saber][$position][$direction] 
                  || $bloq_stats->{opening}[$saber][$position][9];
          }

          if (defined $stats) {
            $note_score = $stats->{func}($time_since_last_note, $note_jump_speed);
            $note_score = 115 if $note_score > 115;
            $note_score = 0 if $note_score < 0;
            $notes_with_stats++;
          } else {
            $note_score = 1;
          }

          if ($note_score == 0) {
            $multiplier = 1;
            $combo = 0;
          } else {
            $combo++;
            $multiplier = 2 if ($combo == 2);
            $multiplier = 4 if ($combo == 6);
            $multiplier = 8 if ($combo == 14);
            $total_score += $multiplier * $note_score;
          }
          $last_times->[$saber] = $time;
          $last_directions->[$saber] = $direction;
          $last_positions->[$saber] = $position;
        }

        my $weight = $notes_with_stats / $note_count;

        $beatmap->{percentage_predicted_notes} = $weight * 100;

        $weight /= 2;

        $total_scores += $weight * ($total_score / $max_score);
        $total_weight += $weight;

        $beatmap->{predicted_score} = $total_scores / $total_weight;
      }
    }
  }
}

my $OptionalPlaylistKeys = [qw(average_score speed_weight songName songAuthorName levelAuthorName percentage_predicted_notes)];

sub writePlaylist {
  my ($beatmaps, $file_name, $title) = @_;

  my $songs;

  foreach my $beatmap (@{$beatmaps}) {
    my $playlist_entry = {
      levelID => $beatmap->{level_id},
      difficulties => [
        {
          characteristic => $beatmap->{game_mode},
          name => $beatmap->{difficulty},
        }
      ],
      predicted_score => $beatmap->{predicted_score},
    };

    foreach my $key (@{$OptionalPlaylistKeys}) {
      $playlist_entry->{$key} = $beatmap->{$key} if exists $beatmap->{$key};
    }

    push @{$songs}, $playlist_entry;
  }

  my $playlist = {
    playlistTitle => $title,
    playlistAuthor => $0,
    songs => $songs,
    image => "",
  };

  open my $fh, ">", catfile($PlayListFolder, $file_name);
  $fh->print(encodeJson $playlist);
  close $fh;
}

sub buildPlaylists {
  my ($beatmaps) = @_;

  my ($songs);

  my ($max_hand_speed) = 0;
  my ($max_hit_speed) = 0;

  my $unplayed;

  my $banned_playlist_file = catfile($PlayListFolder, "bannedForWorkout.bplist");

  if (-f $banned_playlist_file) {
    my $data = decode_json(read_file($banned_playlist_file));

    foreach my $song (@{$data->{songs}}) {
      my $level_id = $song->{levelid} || "custom_level_".$song->{hash};
      next unless defined $level_id;
      foreach my $difficulty_data (@{$song->{difficulties}}) {
        my $gameMode = $difficulty_data->{characteristic} || "Standard";
        my $difficulty = $difficulty_data->{name};
        delete $beatmaps->{$level_id}{$gameMode}{$difficulty};
      }
    }
  }

  my $favourites;

  my $player_data_file = catfile($BeatSaberAppdataFolder, "PlayerData.dat");
  if (-f $player_data_file) {
    my $player_data = decode_json(read_file($player_data_file));

    # TODO read top scores from player_data for 'unplayed' levels?
    #$player_data->{localPlayers}[0]{levelsStatsData}

    foreach my $level_id (@{$player_data->{localPlayers}[0]{favouriteLevelIDs}}) {
      $favourites->{$level_id} = 1;
      my $song_id = $level_id;
      while (exists $SameSongs->{$song_id}) {
        $song_id = $SameSongs->{$song_id};
        $favourites->{$song_id} = 1;
      }
    }
  }

  my ($total_hand_speed, $total_hit_speed, $hand_speed_count);


  foreach my $level_id (keys %{$beatmaps}) {
    my $song_id = $level_id;
    while (exists $SameSongs->{$song_id}) {
      $song_id = $SameSongs->{$song_id};
    }
    foreach my $game_mode (keys %{$beatmaps->{$level_id}}) {
      foreach my $difficulty (keys %{$beatmaps->{$level_id}{$game_mode}}) {
        my $beatmap = $beatmaps->{$level_id}{$game_mode}{$difficulty};
        $beatmap->{song_id} = $song_id;

        if (exists $beatmap->{average_hand_speed}) {
          my $average_hand_speed = $beatmap->{average_hand_speed};
          my $average_hit_speed = $beatmap->{average_hit_speed};
          $total_hand_speed += $average_hand_speed;
          $total_hit_speed += $average_hit_speed;
          $hand_speed_count++;
          $max_hand_speed = $average_hand_speed if ($average_hand_speed > $max_hand_speed);
          $max_hit_speed = $average_hit_speed if ($average_hit_speed > $max_hit_speed);
        }

        $beatmap->{level_id} = $level_id;
        $beatmap->{game_mode} = $game_mode;
        $beatmap->{difficulty} = $difficulty;
        if (!exists $beatmap->{average_score} ) {
          push @{$unplayed}, $beatmap;
          next;
        }

        my $age = ($Now - ($beatmap->{last_played})) / $SecondsPerDay;
        next if $age < 1.0;

        $beatmap->{age} = $age;

        next if $beatmap->{predicted_score} < 0.6;

        push @{$songs->{$song_id}}, $beatmap;
      }
    }
  }

  my ($average_speed_weight) = 1;

  if (defined $hand_speed_count) {
    $average_speed_weight = (($total_hand_speed / $hand_speed_count) / $max_hand_speed) + (($total_hit_speed / $hand_speed_count) / $max_hit_speed);
  }

  my $candidates;
  my $total_weight;

  foreach my $song_id (keys %{$songs}) {
    my $beatmaps = $songs->{$song_id};

    my $max_weight = 0;
    my $victim;

    my $min_age = ${$beatmaps}[0]{age};

    foreach my $beatmap (@{$beatmaps}) {
      my $age = $beatmap->{age};
      $min_age = $age if $age < $min_age;
    }

    foreach my $beatmap (@{$beatmaps}) {
      #my $weight = 1 - $beatmap->{predicted_score};
      #$weight += 1 - $beatmap->{average_score};
      my $weight = $beatmap->{predicted_score} - $beatmap->{average_score};
      my $speed_weight;
      if (defined $beatmap->{average_hand_speed}) {
        $speed_weight = $beatmap->{average_hand_speed} / $max_hand_speed;
        $speed_weight += $beatmap->{average_hit_speed} / $max_hit_speed;
      } else {
        # TODO attempt to predict hand speed?
        $speed_weight = $average_speed_weight;
      }
      $beatmap->{speed_weight} = $speed_weight;
      $weight += $speed_weight;

      next unless $weight > $max_weight;
      $victim = $beatmap;
      $max_weight = $weight;

      $beatmap->{weight} = $weight;
    }

    my $weight = $victim->{weight};

    # play favouries more often
    $weight++ if exists $favourites->{$song_id};

    my $age = $min_age;
    $weight += log($age) / log(10);

    $victim->{weight} = $weight;

    $total_weight += $victim->{weight};

    $candidates->{$song_id} = $victim;
  }

  @{$unplayed} = sort { $b->{predicted_score} <=> $a->{predicted_score} } @{$unplayed};

  my $total_time = 0;
  my $workout;

  if (@{$unplayed}) {
    my $candidate = $unplayed->[0];
    if ($candidate->{predicted_score} > 0.6) {
      shift @{$unplayed};
      $total_time += $candidate->{duration} + 30;
      my $song_id = $candidate->{song_id};
      if (exists $candidates->{$song_id}) {
        my $victim = $candidates->{$song_id};
        my $weight = $victim->{weight};
        $total_weight -= $weight;
        delete $candidates->{$song_id};
      }
      $candidate->{speed_weight} = $average_speed_weight;
      push @$workout, $candidate;
    }
  }

  while($total_time < $WorkoutDuration && keys %{$candidates}) {
    my $pick = rand($total_weight);
    foreach my $song_id (keys %{$candidates}) {
      my $candidate = $candidates->{$song_id};
      my $weight = $candidate->{weight};
      if ($pick < $weight) {
        push @$workout, $candidate;
        $total_time += $candidate->{duration} + 30;
        delete $candidates->{$song_id};
        $total_weight -= $weight;
        last;
      }
      $pick -= $weight;
    }
  }

  writePlaylist(
    $unplayed,
    "unplayed.bplist",
    "Unplayed"
  );

  @{$workout} = sort { $a->{speed_weight} <=> $b->{speed_weight} } @{$workout};

  push @{$workout}, shift @{$workout};

  writePlaylist(
    $workout,
    "workout.bplist",
    "Workout"
  );
}

##############################################################

make_path $DataDir;

make_path $CacheDir;

open STDOUT, ">", catfile($DataDir, "buildBeatSaberPlaylist.txt");
open STDERR, ">&STDOUT";

say "Starting run at ", ts();

my $beatmaps = loadAllBeatMaps();

loadSongPlayHistoryData($beatmaps);

my ($bloq_stats) = {};

loadBeatSaviourData($beatmaps, $bloq_stats);

say "Loaded play data at ", ts();

calculateBloqStats($bloq_stats);

calculateBeatmapStats($beatmaps, $bloq_stats);

buildPlaylists($beatmaps);

say "Run complete at ", ts();
