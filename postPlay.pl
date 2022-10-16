#!perl

use warnings;
use strict;

use v5.16;
use utf8;

use File::Slurp;
use JSON qw(decode_json);
use Cwd qw(abs_path);
use DBI;
use DBD::SQLite;
use File::Spec::Functions qw(catdir catfile updir rel2abs);
use Carp;
use File::stat;
use File::Path qw(make_path);
use POSIX qw(strftime);
use English qw(-no_match_vars);
use autodie qw(:all);

$Carp::Verbose = 1;

my $PlayerID = '76561198001262880';

my $NOW = time();

my $DataDir = catdir(abs_path(__FILE__), updir(), "data");

my $CacheDir = catdir($DataDir, "cache");

my $BeatSaberFolder = "G:\\Steam\\steamapps\\common\\Beat Saber";
my $BSLegacyLauncherFolder = "G:\\Modding\\Beat Saber\\BSLegacyLauncher";
my $BeatSaberFolders = catdir($BSLegacyLauncherFolder, "Installed Versions");

my $BeatSaberAppdataFolder = catdir($ENV{localappdata},updir(),"LocalLow","Hyperbolic Magnetism","Beat Saber");

my $BeatSaviorDataAppdataFolder = catdir($ENV{localappdata},updir(),"Roaming","Beat Savior Data");

my $PlaylistFolder = catdir($BSLegacyLauncherFolder, "Playlists");

my $SongsFolder = catdir($BSLegacyLauncherFolder, "CustomLevels");

my $dbFile = catfile($DataDir, "data.sqlite");

my $dbh = DBI->connect("dbi:SQLite:dbname=$dbFile","","");
$dbh->{HandleError} = sub { Carp::croak(shift) };

$dbh->do("PRAGMA auto_vacuum = INCREMENTAL");
$dbh->do("PRAGMA encoding = 'UTF-8'");
$dbh->do("PRAGMA foreign_keys = ON");
$dbh->do("PRAGMA journal_mode = WAL");
$dbh->do("PRAGMA busy_timeout = 250");
$dbh->do("PRAGMA optimize(0x4)");

sub ts {
  return strftime("%Y-%m-%dT%H:%M:%S", localtime());
}

$dbh->do(<<EOF);
CREATE TABLE IF NOT EXISTS PlayerData (
  ID integer primary key,
  PlayerData TEXT
)
EOF

$dbh->do(<<EOF);
CREATE VIEW IF NOT EXISTS FavouriteLevels AS
select value as LevelId
  from PlayerData,
       json_each(PlayerData, '\$.localPlayers[0].favoritesLevelIds')
EOF

sub loadFavourites {
  my $playerDataFile = catfile($BeatSaberAppdataFolder, "PlayerData.dat");

  return unless -f $playerDataFile;

  say "Reading $playerDataFile at ", ts();

  $dbh->do("insert or replace into PlayerData (id, PlayerData) values (1, json(?))", undef, read_file($playerDataFile));
}

$dbh->do(<<EOF);
CREATE TABLE IF NOT EXISTS VotedSongs (
  SongHash Text primary key,
  VoteType TEXT
)
EOF

$dbh->do(<<EOF);
CREATE TABLE IF NOT EXISTS VotedSongsFiles (
  VersionName Text primary key,
  LastModified TIMESTAMP
)
EOF

sub loadVotedSongs {
  my $getLastModified = $dbh->prepare(<<'EOF');
select LastModified
  from VotedSongsFiles
 where versionName = ?
EOF

  my $applyVotes = $dbh->prepare(<<'EOF');
insert or replace into VotedSongs (SongHash, VoteType)
select key as SongHash,
       json_extract(value, '$.voteType') as VoteType
  from json_each(json(?))
 where json_extract(value, '$.hash') = key
EOF

  my $saveLastModified = $dbh->prepare(<<'EOF');
insert or replace into VotedSongsFiles (VersionName, LastModified)
values (?, ?)
EOF

  my @votedFiles;

  opendir(my $dh, $BeatSaberFolders);

  while (my $versionName = readdir $dh) {
    next if $versionName =~ m/^\./;
    my $votedFile = catfile(catdir($BeatSaberFolders, $versionName, 'UserData'), 'votedSongs.json');
    my $st = stat $votedFile;
    next unless defined $st && -f $st;
    push @votedFiles, [ $versionName, $votedFile, $st->mtime ];
  }

  closedir $dh;

  foreach my $row (sort { $a->[2] <=> $b->[2] } @votedFiles) {
    my ($versionName, $votedFile, $lastModified) = @$row;

    $getLastModified->execute($versionName);
    my $previousLastModified = $getLastModified->fetchrow_array();

    next if $previousLastModified && $lastModified == $previousLastModified;

    say "Reading voted songs from $votedFile at ", ts();

    my $data = read_file($votedFile, { binmode => ':encoding(UTF-8)' });

    # remove byte order mark - why does this have one?
    $data =~ s/^(\x{FEFF})//;
    $row->[4] = $1;

    $applyVotes->execute($data);
  }

  my $data = $dbh->selectall_arrayref(<<'EOF')->[0][0];
select json_group_object(
         SongHash,
         json_object(
           'hash',
           SongHash,
           'voteType',
           VoteType
         )
       )
  from VotedSongs
EOF

  foreach my $row (@votedFiles) {
    my ($versionName, $votedFile, $oldLastModified, $bom) = @$row;

    if ($bom){
      write_file($votedFile, $bom, $data);
    } else {
      write_file($votedFile, $data);
    }

    my $lastModified = (stat $votedFile)->mtime;

    $saveLastModified->execute($versionName, $lastModified);
  }
}

$dbh->do(<<EOF);
CREATE TABLE IF NOT EXISTS BannedSongs (
  LevelID TEXT,
  GameMode TEXT,
  Difficulty TEXT,
  PRIMARY KEY (LevelID, GameMode, Difficulty)
)
EOF

$dbh->do(<<EOF);
CREATE TABLE IF NOT EXISTS BannedSongsFile (
  ID INTEGER PRIMARY KEY,
  LastModified TIMESTAMP
)
EOF

sub loadBannedSongs {
  my $bannedSongsFile = catfile($PlaylistFolder, 'bannedForWorkout.bplist');

  my $stat = stat $bannedSongsFile;

  unless ($stat && -f $stat) {
    saveBannedSongs($bannedSongsFile);
    return;
  }

  my $lastModified = $dbh->selectall_arrayref(<<'EOF')->[0][0];
select LastModified
  from BannedSongsFile
EOF

  return if $lastModified && $lastModified == $stat->mtime;

  say "Reading banned songs from $bannedSongsFile at ", ts();

  eval {
    $dbh->begin_work();

    $dbh->do('delete from BannedSongs');

    $dbh->do(<<'EOF', undef, scalar read_file($bannedSongsFile));
insert into BannedSongs (LevelID, GameMode, Difficulty)
with songs as (
select value as song
  from json_each(json(?1), '$.songs')
)
select json_extract(song, '$.levelid') as levelid,
      json_extract(value, '$.characteristic') as GameMode,
      json_extract(value, '$.name') as Difficulty
  from songs,
      json_each(song, '$.difficulties')
EOF
  };
  if ($EVAL_ERROR) {
    warn $EVAL_ERROR;
    $dbh->rollback();
    return;
  } else {
    $dbh->commit();
  }

  saveBannedSongs($bannedSongsFile);
}

sub saveBannedSongs {
  my ($bannedSongsFile) = @_;

  write_file($bannedSongsFile, $dbh->selectall_arrayref(<<'EOF', undef, $0)->[0][0]);
with grouped as (
  SELECT LevelID,
         json_group_array(
           json_object(
             'characteristic',
             GameMode,
             'name',
             Difficulty
           )
         ) as difficulties
    FROM BannedSongs
group by LevelID
),
objects as (
select case
         when LevelID LIKE 'custom_level_%' THEN json_object(
           'levelid',
           LevelID,
           'hash',
           substr(LevelID,14),
           'difficulties',
           json(difficulties)
         )
	     ELSE json_object(
           'levelid',
           LevelID,
           'difficulties',
           json(difficulties)
         )
	   END as object
  from grouped
)
select json_object(
         'image',
         '',
         'playlistAuthor',
         ?1,
         'playlistTitle',
         'Banned for Workout',
         'songs',
         json_group_array(json(object))
       )
  from objects
EOF

  my $stat = stat $bannedSongsFile;

  my $saveLastModified = $dbh->do(<<'EOF', undef, $stat->mtime);
insert or replace into BannedSongsFile (ID, LastModified)
values (1, ?)
EOF
}

$dbh->do(<<'EOF');
CREATE TEMPORARY TABLE DuplicateSongsLevelIDs (
  ID integer primary key,
  LevelID TEXT,
  Hash TEXT GENERATED ALWAYS AS (CASE WHEN LevelID LIKE 'custom_level_%' THEN substr(LevelID,14) ELSE NULL END) VIRTUAL
)
EOF

$dbh->do(<<'EOF');
CREATE UNIQUE INDEX temp.DuplicateSongsLevelIDs_LevelIDs on DuplicateSongsLevelIDs (
  LevelID
)
EOF

$dbh->do(<<'EOF');
CREATE TEMPORARY TABLE DuplicateSongs (
  ID1 integer,
  ID2 integer,
  PRIMARY KEY (ID1, ID2)
)
EOF

sub loadDuplicateSongs {
  my $duplicatesFolder = catdir($PlaylistFolder, 'Duplicates');

  my $recordLevels = $dbh->prepare(<<'EOF');
insert or ignore into DuplicateSongsLevelIDs (LevelID)
with songs as (
select value as song
  from json_each(json(?1), '$.songs')
),
levelIDs as (
select json_extract(song, '$.levelid') as LevelID
  from songs
)
select distinct levelID
  from levelIDs
EOF

  my $recordDuplicates = $dbh->prepare(<<'EOF');
insert or ignore into DuplicateSongs (ID1, ID2)
with songs as (
select value as song
  from json_each(json(?1), '$.songs')
),
levelIDs as (
select json_extract(song, '$.levelid') as LevelID
  from songs
),
uniqueLevels as (
select distinct levelID
  from levelIDs
),
ids as (
select Q2.ID
  from uniqueLevels Q1
  join DuplicateSongsLevelIDs Q2
    on Q1.LevelID = Q1.LevelID
),
min_id as (
  select min(id) as min_id
    from ids
)
select min_id,
       id
  from min_id,
       ids
 where id <> min_id
EOF

  opendir(my $dh, $duplicatesFolder);

  while (my $file = readdir $dh) {
    next unless $file =~ m/\.bplist$/;
    my $duplicatesFile = catfile($duplicatesFolder, $file);
    my $data = read_file($duplicatesFile);
    $recordLevels->execute($data);
    $recordDuplicates->execute($data);
  }

  closedir $dh;
}

$dbh->do(<<'EOF');
CREATE TABLE IF NOT EXISTS BeatSaviourData (
  FileDate JulianDate,
  RecordNumber INTEGER,
  Data TEXT,
  SongId TEXT GENERATED ALWAYS AS (json_extract(Data,'$.songID')) VIRTUAL,
  GameMode TEXT GENERATED ALWAYS AS (json_extract(Data,'$.gameMode')) VIRTUAL,
  SongDifficultyRank INTEGER GENERATED ALWAYS AS (json_extract(Data,'$.songDifficultyRank')) VIRTUAL,
  RawScore INTEGER GENERATED ALWAYS AS (json_extract(Data,'$.trackers.scoreTracker.rawScore')) VIRTUAL,
  Accuracy NUMBER GENERATED ALWAYS AS (json_extract(Data,'$.trackers.scoreTracker.rawRatio')) VIRTUAL,
  PRIMARY KEY (FileDate, RecordNumber)
)
EOF

$dbh->do(<<'EOF');
CREATE INDEX IF NOT EXISTS BeatSaviourDataIndex1 ON BeatSaviourData (
  SongId,
  GameMode,
  SongDifficultyRank,
  FileDate ASC
)
EOF

$dbh->do(<<EOF);
CREATE TABLE IF NOT EXISTS BeatSaviourDataFiles (
  FileDate JulianDate primary key,
  LastModified TIMESTAMP
)
EOF

sub loadBeatSaviorData {
  my $recordBeatSaviourData = $dbh->prepare(<<'EOF');
insert or replace into BeatSaviourData (FileDate, RecordNumber, Data)
values (julianday(?), ?, json_remove(json(?),'$.deepTrackers'))
EOF

  my $getLastModified = $dbh->prepare(<<'EOF');
select LastModified
  from BeatSaviourDataFiles
 where FileDate = julianday(?)
EOF

  my $saveLastModified = $dbh->prepare(<<'EOF');
insert or replace into BeatSaviourDataFiles (FileDate, LastModified)
values (julianday(?), ?)
EOF

  opendir(my $dh, $BeatSaviorDataAppdataFolder);
  while (my $file = readdir $dh) {
    next unless $file =~ m/(\d\d\d\d-\d\d-\d\d)\.bsd$/;
    my $fileDate = $1;

    my $beatSaviourFile = catfile($BeatSaviorDataAppdataFolder, $file);

    my $st = stat($beatSaviourFile);
    
    next unless $st && -f $st;

    $getLastModified->execute($fileDate);
    my $previousLastModified = $getLastModified->fetchrow_array();

    my $lastModified = $st->mtime;

    next if $previousLastModified && $lastModified == $previousLastModified;

    say "Reading plays from $beatSaviourFile at ", ts();

    open my $in, "<", $beatSaviourFile;

    my $header = <$in>;

    my $recordNumber = 0;

    $dbh->begin_work();

    eval {
      while (my $data = <$in>) {
        $recordBeatSaviourData->execute($fileDate, $recordNumber++, $data);
      }
      $saveLastModified->execute($fileDate, $lastModified);
    };
    if ($EVAL_ERROR) {
      $dbh->rollback();
    } else {
      $dbh->commit();
    }

    close $in;
  }

  closedir $dh;
}

sub pruneBeatSaviorData {
  $dbh->do(<<'EOF');
WITH Numbered AS (
Select ROWID,
       ROW_NUMBER() OVER (PARTITION BY SongId, GameMode, SongDifficultyRank order by FileDate desc) as ROW_NUM
  from BeatSaviourData Q1
)
DELETE FROM BeatSaviourData
 WHERE ROWID IN (
          select ROWID
            from Numbered
           where row_num > 30
                )
EOF

  # beat saviour data keeps the last 30 files.
  # perhaps we should only prune this if the file was not found?
  $dbh->do(<<EOF);
with Numbered as (
select ROWID,
       ROW_NUMBER() over (order by fileDate desc) as row_num
  from BeatSaviourDataFiles
)
delete from BeatSaviourDataFiles
 where ROWID in (
           select ROWID
             from Numbered
            where row_num > 30
                )
EOF
}

sub writeSongsToImprove {

  # TODO factor in the time that a given player earned a given score.

  write_file(catfile($PlaylistFolder, 'to-improve.bplist'), $dbh->selectall_arrayref(<<'EOF', undef, $0)->[0][0]);
with recentScores as (
select SongHash,
       CASE when GameMode like 'Solo%' then substr(GameMode, 5) else GameMode end as GameMode,
       Difficulty,
       1/(JULIANDAY('now') - JULIANDAY(TimeSet)) as Weight,
       BaseScore / cast(MaxScore as real) as Accuracy
  from ScoreSaberScores
 where MaxScore <> 0
   and BaseScore <> 0
union all
select SongId as SongHash,
       COALESCE(GameMode, 'Standard') as GameMode,
       SongDifficultyRank as Difficulty,
       1/(JULIANDAY('now') - FileDate) as Weight,
       Accuracy
  from BeatSaviourData
),
myAccuracy as (
select SongHash,
       GameMode,
       Difficulty,
       sum(Accuracy * Weight) / sum(Weight) as AverageAccuracy 
  from recentScores
 group by SongHash,
          GameMode,
          Difficulty
),
theirAccuracy AS (
Select Q1.LeaderboardID,
       SongHash, 
       GameMode, 
       Difficulty, 
       BaseScore / cast(MaxScore as real) as Accuracy
  from ScoreSaberNeighbourScores Q1
  join NeighboursByScore Q2
    on Q1.PlayerID = Q2.PlayerID
 Where BaseScore <> 0 
   AND MaxScore <> 0
),
theirAccuracyFiltered as (
  Select LeaderboardID,
         Q1.SongHash,
         substr(GameMode,5) as GameMode,
         Difficulty,
         Accuracy
    from theirAccuracy Q1
    join DownloadedSongs Q2
      on Q1.SongHash = Q2.SongHash
   where GameMode Like 'Solo%'
     and Deleted = FALSE
),
theirAverageAccuracy as (
  Select SongHash,
         GameMode,
         Difficulty,
         avg(Accuracy) as AverageAccuracy,
         count(*) as ScoreCount
    from theirAccuracyFiltered
group by LeaderboardID
),
filteredTheirAverageAccuracy as (
select SongHash,
       GameMode,
       Difficulty,
       AverageAccuracy
  from theirAverageAccuracy
 where ScoreCount > 3
),
combined as (
select Q1.SongHash,
       Q1.Difficulty,
       Q1.GameMode,
       Q1.AverageAccuracy as myAccuracy,
       Q2.AverageAccuracy as theirAccuracy
  from myAccuracy Q1
  join filteredTheirAverageAccuracy Q2
    on Q1.SongHash = Q2.SongHash
   and Q1.Difficulty = Q2.Difficulty
   and Q1.GameMode = Q2.GameMode
),
toImprove as (
  select SongHash,
         GameMode,
         CASE Difficulty
           WHEN 1 THEN 'Easy'
           WHEN 3 THEN 'Normal'
           WHEN 5 THEN 'Hard'
           WHEN 7 THEN 'Expert'
           WHEN 9 THEN 'ExpertPlus'
           ELSE Difficulty
         END as Difficulty,
         myAccuracy,
         theirAccuracy
    from combined
   where theirAccuracy > myAccuracy
order by theirAccuracy / myAccuracy desc
)
select json_object(
         'image',
         '',
         'playlistAuthor',
         ?1,
         'playlistTitle',
         'To Improve',
         'songs',
         json_group_array(
           json_object(
             'hash',
             SongHash,
             'difficulties',
             json_array(
               json_object(
                 'characteristic', GameMode,
                 'name', Difficulty
               )
             ),
             'myAccuracy',
             myAccuracy,
             'theirAccuracy',
             theirAccuracy
           )
         )
       )
  from toImprove
EOF
}

sub writeNotPlayedSongs {
  write_file(catfile($PlaylistFolder, 'not-played.bplist'), $dbh->selectall_arrayref(<<'EOF', undef, $0)->[0][0]);
with recentScores as (
select SongHash,
       CASE when GameMode like 'Solo%' then substr(GameMode, 5) else GameMode end as GameMode,
       Difficulty
  from ScoreSaberScores
union all
select SongId as SongHash,
       COALESCE(GameMode, 'Standard') as GameMode,
       SongDifficultyRank as Difficulty
  from BeatSaviourData
),
SongsWeHavePlayed as (
select SongHash,
       GameMode,
       Difficulty 
  from recentScores
 group by SongHash,
          GameMode,
          Difficulty
),
theirAccuracy AS (
Select Q1.LeaderboardID,
       SongHash, 
       GameMode, 
       Difficulty, 
       BaseScore / cast(MaxScore as real) as Accuracy
  from ScoreSaberNeighbourScores Q1
  join NeighboursByScore Q2
    on Q1.PlayerID = Q2.PlayerID
 Where BaseScore <> 0 
   AND MaxScore <> 0
),
theirAccuracyFiltered as (
  Select LeaderboardID,
         Q1.SongHash,
         substr(GameMode,5) as GameMode,
         Difficulty,
         Accuracy
    from theirAccuracy Q1
    join DownloadedSongs Q2
      on Q1.SongHash = Q2.SongHash
   where GameMode Like 'Solo%'
     and Deleted = FALSE
),
theirAverageAccuracy as (
  Select SongHash,
         GameMode,
         Difficulty,
         avg(Accuracy) as AverageAccuracy,
         count(*) as ScoreCount
    from theirAccuracyFiltered
group by LeaderboardID
),
filteredTheirAverageAccuracy as (
select SongHash,
       GameMode,
       Difficulty,
       AverageAccuracy
  from theirAverageAccuracy
 where ScoreCount >= 3
),
combined as (
   select Q1.SongHash,
          Q1.Difficulty,
          Q1.GameMode,
          Q1.AverageAccuracy as PredictedAccuracy
     from filteredTheirAverageAccuracy Q1
left join SongsWeHavePlayed Q2
       on Q1.SongHash = Q2.SongHash
      and Q1.Difficulty = Q2.Difficulty
      and Q1.GameMode = Q2.GameMode
    where Q2.SongHash is NULL
),
newSongs as (
  select SongHash,
         GameMode,
         CASE Difficulty
           WHEN 1 THEN 'Easy'
           WHEN 3 THEN 'Normal'
           WHEN 5 THEN 'Hard'
           WHEN 7 THEN 'Expert'
           WHEN 9 THEN 'ExpertPlus'
           ELSE Difficulty
         END as Difficulty,
         PredictedAccuracy
    from combined
order by PredictedAccuracy DESC
)
select json_object(
         'image',
         '',
         'playlistAuthor',
         ?1,
         'playlistTitle',
         'Not Played',
         'songs',
         json_group_array(
           json_object(
             'hash',
             SongHash,
             'difficulties',
             json_array(
               json_object(
                 'characteristic', GameMode,
                 'name', Difficulty
               )
             ),
             'predictedAccuracy',
             PredictedAccuracy
           )
         )
       )
  from newSongs
EOF
}

$dbh->do(<<'EOF');
CREATE VIEW IF NOT EXISTS WorkoutPlan AS
with recentScores as (
select SongHash,
       CASE when GameMode like 'Solo%' then substr(GameMode, 5) else GameMode end as GameMode,
       Difficulty,
       JULIANDAY('now') - JULIANDAY(TimeSet) as Age,
       1/(JULIANDAY('now') - JULIANDAY(TimeSet)) as Weight,
       BaseScore / cast(MaxScore as real) as Accuracy
  from ScoreSaberScores
 where MaxScore <> 0
   and BaseScore <> 0
union all
select SongId as SongHash,
       COALESCE(GameMode, 'Standard') as GameMode,
       SongDifficultyRank as Difficulty,
       JULIANDAY('now') - FileDate as Age,
       1/(JULIANDAY('now') - FileDate) as Weight,
       Accuracy
  from BeatSaviourData
),
recentSongs as (
select SongHash,
       min(Age) as Age
  from recentScores
 group by SongHash
),
myAccuracy as (
select SongHash,
       GameMode,
       Difficulty,
       sum(Accuracy * Weight) / sum(Weight) as AverageAccuracy 
  from recentScores
 group by SongHash,
          GameMode,
          Difficulty
),
theirAccuracy AS (
Select Q1.LeaderboardID,
       SongHash, 
       GameMode, 
       Difficulty, 
       BaseScore / cast(MaxScore as real) as Accuracy
  from ScoreSaberNeighbourScores Q1
  join NeighboursByScore Q2
    on Q1.PlayerID = Q2.PlayerID
 Where BaseScore <> 0 
   AND MaxScore <> 0
),
theirAccuracyFiltered as (
  Select LeaderboardID,
         Q1.SongHash,
         substr(GameMode,5) as GameMode,
         Difficulty,
         Accuracy,
         json_extract(BeatSaverData, '$.metadata.duration') as Duration
    from theirAccuracy Q1
    join DownloadedSongs Q2
      on Q1.SongHash = Q2.SongHash
   where GameMode Like 'Solo%'
     and Deleted = FALSE
),
theirAverageAccuracy as (
  Select SongHash,
         GameMode,
         Difficulty,
         avg(Accuracy) as AverageAccuracy,
         count(*) as ScoreCount,
         Duration
    from theirAccuracyFiltered
group by LeaderboardID
),
filteredTheirAverageAccuracy as (
select SongHash,
       GameMode,
       Difficulty,
       AverageAccuracy,
       Duration
  from theirAverageAccuracy
 where ScoreCount > 3
),
beatSaviourStats0 as (
select SongId as SongHash,
       COALESCE(GameMode, 'Standard') as GameMode,
       SongDifficultyRank as Difficulty,
       1/(JULIANDAY('now') - FileDate) as Weight,
       json_extract(Data, '$.trackers.distanceTracker.rightSaber') + json_extract(Data, '$.trackers.distanceTracker.leftSaber') as SaberDistance,
       json_extract(Data, '$.trackers.distanceTracker.rightHand') + json_extract(Data, '$.trackers.distanceTracker.leftHand') as HandDistance,
       json_extract(Data, '$.songDuration') as SongDuration
  from BeatSaviourData
),
beatSaviourStats1 as (
select SongHash,
       GameMode,
       Difficulty,
       Weight,
       SaberDistance / SongDuration as SaberSpeed,
       HandDistance / SongDuration as HandSpeed
  from BeatSaviourStats0
),
beatSaviourStats as (
  select SongHash,
         GameMode,
         Difficulty,
         sum(SaberSpeed * Weight) / sum(Weight) as SaberSpeed,
         sum(HandSpeed * Weight) / sum(Weight) as HandSpeed
    from beatSaviourStats1
group by SongHash,
         GameMode,
         Difficulty
),
favouriteSongs as (
SELECT substr(LevelId,14) as SongHash
  FROM FavouriteLevels
 where LevelId like 'custom_level_%'
),
combined as (
   select Q1.SongHash,
          Q1.Difficulty,
          Q1.GameMode,
          Q2.AverageAccuracy as myAccuracy,
          Q1.AverageAccuracy as theirAccuracy,
          Q5.Age,
          Q3.SaberSpeed,
          Q3.HandSpeed,
          CASE WHEN Q4.SongHash is not NULL THEN 1 ELSE 0 END as IsFavorite,
          Q1.Duration
     from filteredTheirAverageAccuracy Q1
left join myAccuracy Q2
       on Q1.SongHash = Q2.SongHash
      and Q1.Difficulty = Q2.Difficulty
      and Q1.GameMode = Q2.GameMode
left join beatSaviourStats Q3
       on Q1.SongHash = Q3.SongHash
      and Q1.Difficulty = Q3.Difficulty
      and Q1.GameMode = Q3.GameMode
left join favouriteSongs Q4
       on Q1.SongHash = Q4.SongHash
left join recentSongs Q5
       on Q1.SongHash = Q5.SongHash
),
calculations as (
select SongHash,
       Difficulty,
       GameMode,
       CASE
         WHEN theirAccuracy >= myAccuracy
           THEN theirAccuracy / myAccuracy
         ELSE NULL
       END as PotentialImprovement,
       theirAccuracy as PotentialScore,
       Age,
       HandSpeed / SaberSpeed as WristFactor,
       HandSpeed,
       IsFavorite,
       Duration,
       abs((3 * 60) - Duration) as OffsetFromDesiredTime
  from combined
 where (Age is NULL or AGE > 1)
),
averages as (
select avg(WristFactor) as AverageWristFactor,
       avg(HandSpeed) as AverageHandSpeed
  from calculations
),
ranked as (
  select SongHash,
         Difficulty,
         GameMode,
         PotentialImprovement,
         percent_rank() over (order by PotentialImprovement asc nulls first) as ImprovementRank,
         PotentialScore,
         percent_rank() over (order by PotentialScore asc nulls first) as PotentialScoreRank,
         Age,
         percent_rank() over (order by Age asc nulls last) as AgeRank,
         WristFactor,
         percent_rank() over (order by COALESCE(WristFactor, AverageWristFactor) asc) as WristFactorRank,
         HandSpeed,
         percent_rank() over (order by COALESCE(HandSpeed, AverageHandSpeed) asc) as HandSpeedRank,
         IsFavorite,
         strftime('%M:%S', Duration, 'unixepoch') as Duration,
         percent_rank() over (order by OffsetFromDesiredTime desc) as DurationRank
    from calculations,
         averages
)
select Q1.*,
         (ImprovementRank + 0.01)
       * (PotentialScoreRank + 0.01)
       * (AgeRank + 0.01)
       * (WristFactorRank + 0.01)
       * (HandSpeedRank + 0.01)
       * (IsFavorite + 1)
       * (DurationRank + 0.01) as CombinedRank
    from ranked Q1
order by CombinedRank desc
EOF

# TODO weight by:
# * how long the song is

sub writeWorkout {

  $dbh->do(<<'EOF');
CREATE TEMPORARY TABLE Workout (
  SongHash TEXT PRIMARY KEY,
  GameMode TEXT,
  Difficulty INTEGER,
  HandSpeed NUMBER
)
EOF

  my $workoutCandidates = $dbh->prepare(<<'EOF');
Select SongHash,
       GameMode,
       Difficulty,
       HandSpeed
  from WorkoutPlan
 where Age is NOT NULL
EOF

  $dbh->do(<<'EOF');
INSERT INTO Workout (SongHash, GameMode, Difficulty, HandSpeed)
SELECT SongHash,
       GameMode,
       Difficulty,
       HandSpeed
  from WorkoutPlan
 where Age is NULL
 limit 1
EOF

  my $getIsDuplicate = $dbh->prepare(<<'EOF');
With Selected as (
SELECT SongHash
  FROM Workout
),
Direct as (
    SELECT Q4.Hash as SongHash
     FROM Workout Q1
     join DuplicateSongsLevelIDs Q2 on Q1.SongHash = Q2.Hash
left join DuplicateSongs Q3 on Q3.ID1 = Q2.ID
left join DuplicateSongsLevelIDs Q4 on Q3.ID2 = Q4.ID
),
Indirect as (
    SELECT Q5.Hash as SongHash
     FROM Workout Q1
     join DuplicateSongsLevelIDs Q2 on Q1.SongHash = Q2.Hash
left join DuplicateSongs Q3 on Q3.ID2 = Q2.ID
left join DuplicateSongs Q4 on Q3.ID1 = Q4.ID1
left join DuplicateSongsLevelIDs Q5 on Q4.ID2 = Q5.ID
),
Combined as (
Select SongHash FROM Selected
UNION ALL
Select SongHash FROM Direct
UNION ALL
Select SongHash FROM Indirect
)
SELECT count(*)
  FROM Combined
 WHERE SongHash = ?
EOF

  my $addToWorkout = $dbh->prepare(<<'EOF');
INSERT INTO Workout (SongHash, GameMode, Difficulty, HandSpeed)
VALUES (?, ?, ?, ?)
EOF

  my $getWorkoutDuration = $dbh->prepare(<<'EOF');
SELECT sum(json_extract(BeatSaverData, '$.metadata.duration'))
  FROM Workout Q1
  join DownloadedSongs Q2
    on Q1.SongHash = Q2.SongHash
EOF

  $workoutCandidates->execute();

  my $targetWorkoutDuration = 40 * 60;

  while (my ($songHash, $gameMode, $difficulty, $handSpeed) = $workoutCandidates->fetchrow_array()) {
    say "Considering adding ($songHash, $gameMode, $difficulty) to Workout";
    $getIsDuplicate->execute($songHash);
    my $isDuplicate = $getIsDuplicate->fetchrow_array();
    next if $isDuplicate;

    $addToWorkout->execute($songHash, $gameMode, $difficulty, $handSpeed);

    $getWorkoutDuration->execute();
    my $workoutDuration = $getWorkoutDuration->fetchrow_array();
    say "Workout is now $workoutDuration seconds long";
    last if $workoutDuration >= $targetWorkoutDuration;
  }

  my $workoutFile = catfile($PlaylistFolder, 'workout.bplist');

  write_file($workoutFile, $dbh->selectall_arrayref(<<'EOF', undef, $0)->[0][0]);
with ordered1 as (
select SongHash,
       GameMode,
       CASE Difficulty
         WHEN 1 THEN 'Easy'
         WHEN 3 THEN 'Normal'
         WHEN 5 THEN 'Hard'
         WHEN 7 THEN 'Expert'
         WHEN 9 THEN 'ExpertPlus'
         ELSE Difficulty
       END as Difficulty,
       ROW_NUMBER() OVER (ORDER BY HandSpeed asc) as row_num
  from Workout
),
ordered2 as (
  select SongHash,
         GameMode,
         Difficulty,
         CASE row_num
           WHEN 1
             THEN NULL
           ELSE row_num
         END as row_num2
    from ordered1
order by row_num2 desc nulls last
)
select json_object(
         'image',
         '',
         'playlistAuthor',
         ?1,
         'playlistTitle',
         'Workout',
         'songs',
         json_group_array(
           json_object(
             'hash',
             SongHash,
             'difficulties',
             json_array(
               json_object(
                 'characteristic', GameMode,
                 'name', Difficulty
               )
             )
           )
         )
       )
  from ordered2
EOF
}

##############################################################

make_path $DataDir;

open STDOUT, ">", catfile($DataDir, "postPlay.txt");
open STDERR, ">&STDOUT";
STDOUT->autoflush(1);

say "Starting run at ", ts();

loadFavourites();
loadVotedSongs();

loadBannedSongs();
loadDuplicateSongs();

loadBeatSaviorData();
pruneBeatSaviorData();

writeSongsToImprove();
writeNotPlayedSongs();
writeWorkout();

say "Run complete at ", ts();

