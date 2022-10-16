#!perl

use warnings;
use strict;

use v5.16;
use utf8;

use JSON qw(decode_json encode_json);
use Cwd qw(abs_path);
use DBI qw(SQL_NUMERIC);
use DBD::SQLite;
use File::Spec::Functions qw(catdir catfile updir rel2abs);
use Carp;
use LWP::RobotUA;
use LWP::ConnCache;
use HTTP::Cache::Transparent;
use POSIX qw(ceil floor strftime);
use Time::HiRes qw(time sleep);
use File::Path qw(make_path remove_tree);
use Digest::SHA1;
use File::Slurp;
use Archive::Zip;
use English qw(-no_match_vars);
use autodie qw(:all);

$Carp::Verbose = 1;

my $PlayerID = '76561198001262880';

my $NOW = time();

my $SECONDS_PER_DAY = 24 * 60 * 60;
my $SECONDS_IN_15_MINUTES = 15 * 60;
my $SECONDS_IN_SIX_MONTHS = 6 * 30 * $SECONDS_PER_DAY;
my $SECONDS_PER_WEEK = $SECONDS_PER_DAY * 7;

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
$dbh->{HandleError} = sub { confess(shift) };

$dbh->do("PRAGMA auto_vacuum = INCREMENTAL");
$dbh->do("PRAGMA encoding = 'UTF-8'");
$dbh->do("PRAGMA foreign_keys = ON");
$dbh->do("PRAGMA journal_mode = WAL");
$dbh->do("PRAGMA busy_timeout = 250");
$dbh->do("PRAGMA optimize(0x4)");
#$dbh->do("PRAGMA page_size = 65536");

HTTP::Cache::Transparent::init({
  BasePath => catdir($CacheDir, "http"),
  MaxAge => 8*24,
  Verbose => 1,
  NoUpdate => $SECONDS_IN_15_MINUTES,
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

sub ts {
  return strftime("%Y-%m-%dT%H:%M:%S", localtime());
}

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

$dbh->do(<<'EOF');
CREATE TABLE IF NOT EXISTS ScoreSaberScores (
  ID INTEGER PRIMARY KEY,
  Data TEXT,
  LeaderBoardID TEXT GENERATED ALWAYS AS (json_extract(Data,'$.leaderboard.id')) VIRTUAL,
  BaseScore INTEGER GENERATED ALWAYS AS (json_extract(Data, '$.score.baseScore')) VIRTUAL,
  Rank INTEGER GENERATED ALWAYS AS (json_extract(Data, '$.score.rank')) VIRTUAL,
  MaxScore INTEGER GENERATED ALWAYS AS (json_extract(Data, '$.leaderboard.maxScore')),
  SongHash TEXT GENERATED ALWAYS AS (json_extract(Data, '$.leaderboard.songHash')) VIRTUAL,
  GameMode TEXT GENERATED ALWAYS AS (json_extract(Data, '$.leaderboard.difficulty.gameMode')) VIRTUAL,
  Difficulty INTEGER GENERATED ALWAYS AS (json_extract(Data, '$.leaderboard.difficulty.difficulty')) VIRTUAL,
  TimeSet TEXT GENERATED ALWAYS AS (json_extract(Data, '$.score.timeSet')) VIRTUAL
)
EOF

$dbh->do(<<'EOF');
CREATE INDEX IF NOT EXISTS ScoreSaberScores_LeaderboardID ON ScoreSaberScores (
  LeaderboardID
)
EOF

sub fetchScores {
  my $baseUrl = "https://scoresaber.com/api/player/${PlayerID}/scores?sort=recent";

  my $res = get($baseUrl . "&withMetadata=true&limit=100");

  my $page = 1;
  my $done = 0;

  my $meta = decode_json($res);

  my $itemsPerPage = $meta->{metadata}{itemsPerPage};

  $baseUrl .= "&withMetadata=false&limit=${itemsPerPage}";

  my $pages = ceil($meta->{metadata}{total} / $itemsPerPage);

  undef $meta;

  my $record = $dbh->prepare(<<'EOF');
with playerScores as (
  SELECT value as playerScore
    from json_each(json(?), '$.playerScores')
)
insert or replace into ScoreSaberScores (ID, Data)
select json_extract(playerScore, '$.score.id'),
       playerScore
  from playerScores
EOF

  while (1) {
    $record->execute($res);

    $page++;
    last if $page > $pages;

    $res = get($baseUrl . "&page=$page");
  }

  $record->finish();
}

$dbh->do(<<'EOF');
CREATE TABLE IF NOT EXISTS ScoreSaberLeaderboardScores (
  ScoreID INTEGER PRIMARY KEY,
  LeaderboardID INTEGER,
  Data TEXT,
  PlayerID TEXT GENERATED ALWAYS AS (json_extract(Data,'$.leaderboardPlayerInfo.id')) STORED,
  BaseScore TEXT GENERATED ALWAYS AS (json_extract(Data,'$.baseScore')) VIRTUAL
)
EOF

$dbh->do(<<'EOF');
CREATE INDEX IF NOT EXISTS ScoreSaberLeaderboardScores_LeaderboardID ON ScoreSaberLeaderboardScores (
  LeaderboardID
)
EOF

$dbh->do(<<'EOF');
CREATE VIEW IF NOT EXISTS NeighboursByScore AS
With myScores as (
SELECT LeaderboardID,
       BaseScore as myScore
  from ScoreSaberScores
),
neighbourScores as (
SELECT PlayerID,
       LeaderboardID,
       BaseScore as neighbourScore
  from ScoreSaberLeaderboardScores
),
scoreRatios as (
SELECT Q2.PlayerID,
       abs((neighbourScore / cast(myScore as real)) - 1) as scoreRatio
  from myScores Q1
  join neighbourScores Q2
    on Q1.LeaderboardID = Q2.LeaderboardID
),
counted as (
  SELECT PlayerID,
         count(*) as commonLeaderboardCount
    from scoreRatios
   where scoreRatio < 0.1
group by PlayerID
)
select PlayerID,
       commonLeaderboardCount
  from counted
 where commonLeaderboardCount > 3
EOF

sub fetchLeaderboards {
  my $recordScores = $dbh->prepare(<<'EOF');
with scores as (
  SELECT value as score
    from json_each(json(?1), '$.scores')
)
insert or replace into ScoreSaberLeaderboardScores (ScoreID, LeaderboardID, Data)
select json_extract(score, '$.id'),
       ?2,
       score
  from scores
 where json_extract(score, '$.leaderboardPlayerInfo.id') <> ?3
EOF

  my $countNeighbours = $dbh->prepare(<<'EOF');
  select count(*)
    from NeighboursByScore
EOF

  my $getScoresBelowCutoff = $dbh->prepare(<<'EOF');
with scores as (
  SELECT value as score
    from json_each(json(?1), '$.scores')
),
extracted as (
  SELECT json_extract(score, '$.baseScore') as score
    from scores
),
filtered as (
  SELECT score
    from extracted
   where score > 0
)
select count(*)
  from filtered
 where score <= cast(?2 as integer)
EOF

  my $itemsPerPage = 12;

  my $checkItemsPerPage = 1;

  my $leaderboards = $dbh->selectall_arrayref(<<'EOF');
  select LeaderboardID,
         BaseScore,
         Rank
    from ScoreSaberScores
EOF

  my $nextLeaderboards;

  foreach my $leaderboard_data (@$leaderboards) {
    my ($leaderboardId, $baseScore, $rank) = @$leaderboard_data;

    my $page = ceil($rank / $itemsPerPage);

    if ($checkItemsPerPage) {
      my $res = get("https://scoresaber.com/api/leaderboard/by-id/${leaderboardId}/scores?page=${page}");

      next if length($res) == 0;

      my $meta = decode_json($res);
      my $newItemsPerPage = $meta->{metadata}{itemsPerPage};
      if ($newItemsPerPage != $itemsPerPage) {
        say "itemsPerPage for scores is now $newItemsPerPage!";
        $itemsPerPage = $newItemsPerPage;

        $page = ceil($rank / $itemsPerPage);
      }
      $checkItemsPerPage = 0;
    }

    push @$nextLeaderboards, [ $leaderboardId, floor($baseScore * 0.9), $page ];

  }

  my $neighbourCount = 0;

  $recordScores->bind_param(3, $PlayerID);
  
  do {{
    @$leaderboards = @$nextLeaderboards;
    @$nextLeaderboards = ();
    foreach my $leaderboard_data (@$leaderboards) {
      my ($leaderboardId, $cutoffScore, $page) = @$leaderboard_data;

      my $res = get("https://scoresaber.com/api/leaderboard/by-id/${leaderboardId}/scores?page=${page}");

      next if length($res) == 0;

      $recordScores->bind_param(1, $res);
      $recordScores->bind_param(2, $leaderboardId, SQL_NUMERIC);
      $recordScores->execute();

      $getScoresBelowCutoff->execute($res, $cutoffScore);

      my ($scoresBelowCutoff) = $getScoresBelowCutoff->fetchrow_array();

      if ($scoresBelowCutoff > 0) {
        say "found $scoresBelowCutoff scores below the cutoff (our score - 10% = $cutoffScore), not fetching more scores from this leaderboard.";

        next;
      }

      $leaderboard_data->[2]++;
      push @$nextLeaderboards, $leaderboard_data;
    }

    $countNeighbours->execute();
    ($neighbourCount) = $countNeighbours->fetchrow_array();
    say "We now have $neighbourCount neighbours";

    sleep 1;
  }} while ($neighbourCount < 200 && @$nextLeaderboards);
}

sub pruneLeaderboards {
  # Delete scores on the same leaderboard that are greater than +/-20% from our score.
  $dbh->do(<<'EOF');
delete from ScoreSaberLeaderboardScores as Q1
 where abs((Q1.BaseScore / (
  select cast(Q2.BaseScore as real)
    from ScoreSaberScores Q2
   WHERE Q2.LeaderboardID = Q1.LeaderboardID
  )) - 1) > 0.2
EOF
}

$dbh->do(<<'EOF');
CREATE TABLE IF NOT EXISTS ScoreSaberNeighbourScores (
  ID INTEGER PRIMARY KEY,
  PlayerID TEXT,
  Data TEXT,
  LeaderboardID NUMBER GENERATED ALWAYS AS (json_extract(Data, '$.leaderboard.id')) VIRTUAL,
  BaseScore NUMBER GENERATED ALWAYS AS (json_extract(Data, '$.score.baseScore')) VIRTUAL,
  MaxScore NUMBER GENERATED ALWAYS AS (json_extract(Data, '$.leaderboard.maxScore')) VIRTUAL,
  SongHash TEXT GENERATED ALWAYS AS (json_extract(Data, '$.leaderboard.songHash')) VIRTUAL,
  GameMode TEXT GENERATED ALWAYS AS (json_extract(Data, '$.leaderboard.difficulty.gameMode')) VIRTUAL,
  Difficulty TEXT GENERATED ALWAYS AS (json_extract(Data, '$.leaderboard.difficulty.difficulty')) VIRTUAL,
  TimeSet TEXT GENERATED ALWAYS AS (json_extract(Data, '$.score.timeSet')) VIRTUAL
)
EOF

$dbh->do(<<'EOF');
CREATE INDEX IF NOT EXISTS ScoreSaberNeighbourScores_LeaderboardID ON ScoreSaberNeighbourScores (
  LeaderboardID
)
EOF

sub fetchNeighbours() {
  my $findNeighbours = $dbh->prepare(<<'EOF');
  select PlayerID,
         commonLeaderboardCount
    from NeighboursByScore
order by commonLeaderboardCount DESC
   limit 200
EOF

  my $recordNeighbourScores = $dbh->prepare(<<'EOF');
with playerScores as (
  SELECT value as playerScore
    from json_each(json(?1), '$.playerScores')
)
insert or replace into ScoreSaberNeighbourScores (ID, PlayerID, Data)
select json_extract(playerScore, '$.score.id'),
       ?2,
       playerScore
  from playerScores
EOF

  $findNeighbours->execute();

  while (my ($neighbourPlayerId, $count) = $findNeighbours->fetchrow_array()) {
    say "fetching top scores for player $neighbourPlayerId with who we share $count leaderboards";
    my $res = get("https://scoresaber.com/api/player/${neighbourPlayerId}/scores?limit=100&sort=top");

    $recordNeighbourScores->execute($res, $neighbourPlayerId);
  }

  $findNeighbours->finish();
  $recordNeighbourScores->finish();
}

sub pruneNeighbours {
  $dbh->do(<<'EOF');
With myScores as (
SELECT LeaderboardID,
       BaseScore as myScore
  from ScoreSaberScores
),
neighbourScores as (
SELECT PlayerID,
       LeaderboardID,
       BaseScore as neighbourScore
  from ScoreSaberLeaderboardScores
),
scoreRatios as (
SELECT Q2.PlayerID,
       abs((neighbourScore / cast(myScore as real)) - 1) as scoreRatio
  from myScores Q1
  join neighbourScores Q2
    on Q1.LeaderboardID = Q2.LeaderboardID
),
counted as (
  SELECT PlayerID,
         count(*) as commonLeaderboardCount
    from scoreRatios
   where scoreRatio < 0.15
group by PlayerID
),
potentialNeighbours as (
select PlayerID
  from counted
 where commonLeaderboardCount > 1
)
delete 
--select distinct PlayerID
  from ScoreSaberNeighbourScores
 where PlayerID not in (
                  select PlayerID
                    from potentialNeighbours
                       )
EOF
}

$dbh->do(<<'EOF');
CREATE TABLE IF NOT EXISTS DownloadedSongs (
  SongHash TEXT PRIMARY KEY,
  SongDir TEXT,
  BeatSaverData TEXT,
  Deleted INTEGER
)
EOF

$dbh->do(<<'EOF');
CREATE UNIQUE INDEX IF NOT EXISTS DownloadedSongs_SongDir ON DownloadedSongs (
  SongDir
)
EOF

$dbh->do(<<'EOF');
CREATE TEMPORARY TABLE SeenSongs (
  SongDir TEXT,
  SongHash TEXT
)
EOF

sub loadDownloadedSongs {
  my $getHaveHash = $dbh->prepare(<<'EOF');
select count(*)
  from DownloadedSongs
 where SongDir = ?
EOF

  my $recordHash = $dbh->prepare(<<'EOF');
INSERT INTO SeenSongs (SongHash, SongDir)
VALUES (?, ?)
EOF

  my $recordSeen = $dbh->prepare(<<'EOF');
INSERT INTO SeenSongs (SongDir)
VALUES (?)
EOF

  my $digest = Digest::SHA1->new();

  opendir(my $dh, $SongsFolder);

  while (my $name = readdir $dh) {
    next if $name =~ m/^\./;

    my $songDir = catdir($SongsFolder, $name);

    next unless -d $songDir;

    my $infoFile = catfile($songDir, 'Info.dat');

    next unless -f $infoFile;

    $getHaveHash->execute($name);
    my ($haveHash) = $getHaveHash->fetchrow_array();

    if ($haveHash) {
      $recordSeen->execute($name);
      next;
    }

    eval {
      my $infoText = read_file($infoFile, { binmode => ':raw' });

      $digest->add($infoText);

      my $info = decode_json($infoText);

      foreach my $difficultyBeatmapsSet (@{$info->{'_difficultyBeatmapSets'}}) {
        foreach my $difficultyBeatmap (@{$difficultyBeatmapsSet->{_difficultyBeatmaps}}) {
          my $beatmapFilename = $difficultyBeatmap->{_beatmapFilename};
          my $beatmapFilePath = catfile($songDir, $beatmapFilename);
          open my $beatmapFh, "<:raw", $beatmapFilePath;
          $digest->addfile($beatmapFh);
        }
      }

      my $songHash = uc $digest->hexdigest();
      $digest = $digest->new();

      say "found $name with hash $songHash";

      $recordHash->execute($songHash, $name);
    };
    if ($EVAL_ERROR) {
      warn $EVAL_ERROR;
      next;
    }
  }

  closedir($dh);

  foreach my $row (@{$dbh->selectall_arrayref(<<'EOF')}) {
With Hashes as (
   SELECT COALESCE(Q1.SongHash, Q2.SongHash) as SongHash,
          Q1.SongDir
     FROM SeenSongs Q1
LEFT JOIN DownloadedSongs Q2
       ON Q1.SongDir = Q2.SongDir
),
Counted as (
  SELECT SongHash,
         json_group_array(SongDir) as SongDirs,
         count(*) as SongCount
    FROM Hashes
GROUP BY SongHash
)
SELECT SongHash,
       SongDirs
  FROM Counted
 WHERE SongCount > 1
EOF
  my ($songHash, $songDir) = @{$row};

  say "Found duplicate $songHash";
  my $songDirs = decode_json($songDir);

  my $keptDir = shift @$songDirs;
  say "keeping $keptDir";

  foreach my $songDir (@{$songDirs}) {
    my $songPath = catdir($SongsFolder, $songDir);
    say "removing $songPath";
    remove_tree($songPath);
  }
  print "\n";
}

  $dbh->do(<<'EOF');
INSERT INTO DownloadedSongs (SongHash, SongDir)
   SELECT SongHash, SongDir
     FROM SeenSongs
 GROUP BY SongHash
ON CONFLICT(SongHash) DO UPDATE SET SongDir = excluded.SongDir
ON CONFLICT(SongDir) DO NOTHING
EOF

  $dbh->do(<<'EOF');
UPDATE DownloadedSongs as Q1
   SET Deleted = 1 - (
  SELECT COUNT(*)
    FROM SeenSongs Q2
   WHERE Q1.SongDir = Q2.SongDir
   )
EOF
}

sub fetchBeatsaverData {
  my $hashesToFetch = $dbh->prepare(<<'EOF');
SELECT SongHash
  From DownloadedSongs
 Where Deleted = FALSE
   and BeatSaverData is NULL
EOF

  my $storeData = $dbh->prepare(<<'EOF');
UPDATE DownloadedSongs
   SET BeatSaverData = json(?2)
 WHERE SongHash = ?1
EOF

  $hashesToFetch->execute();

  while (my $hashToFetch = $hashesToFetch->fetchrow_array) {
    my $res;
    eval {
      $res = get("https://api.beatsaver.com/maps/hash/$hashToFetch");
    };
    if ($EVAL_ERROR) {
      if ($EVAL_ERROR =~ m/404 Not Found/) {
        warn $EVAL_ERROR;
        next;
      }
      die $EVAL_ERROR;
    }
    $storeData->execute($hashToFetch, $res);
  }
}

sub renameDownloadedSongs {
  my $namesToFix = $dbh->prepare(<<'EOF');
with MapNames as (
select SongDir,
       json_extract(BeatSaverData, '$.id') || ' (' || json_extract(BeatSaverData, '$.metadata.songName') || ' - ' || json_extract(BeatSaverData, '$.metadata.levelAuthorName') || ')' as MapName
  from DownloadedSongs ds
 where BeatSaverData is not NULL 
)
select SongDir, MapName
  from MapNames
 where SongDir <> MapName
EOF

  $namesToFix->execute();
  while (my ($songDir, $mapName) = $namesToFix->fetchrow_array()) {
    $mapName =~ s{[<>:/\\|?*"\x00-\x1f].*$}{};
    next if lc $songDir eq lc $mapName;
    say "renaming $songDir to $mapName";
    my $oldPath = catdir($SongsFolder, $songDir);
    my $newPath = catdir($SongsFolder, $mapName);
    eval {
      rename $oldPath, $newPath;
    };
    if ($EVAL_ERROR) {
      warn $EVAL_ERROR;
    }
  }
}

sub fetchNewSongs {
  my $songsToDownload = $dbh->prepare(<<'EOF');
With favouriteSongs as (
select substr(LevelId,14) as SongHash
  from FavouriteLevels
 where LevelId LIKE 'custom_level_%'
),
myFavouriteLeaderboards as (
SELECT LeaderboardID
  from ScoreSaberScores Q1
  join FavouriteSongs Q2
    on Q1.SongHash = Q2.SongHash
),
neighbourLeaderboards as (
SELECT PlayerID,
       LeaderboardID
  from ScoreSaberLeaderboardScores
),
commonLeaderboards as (
SELECT Q2.PlayerID
  from myFavouriteLeaderboards Q1
  join neighbourLeaderboards Q2
    on Q1.LeaderboardID = Q2.LeaderboardID
),
counted as (
  SELECT PlayerID,
         count(*) as commonLeaderboardCount
    from commonLeaderboards
group by PlayerID
),
playersThatLikeSongsWeDo as (
select PlayerID,
       commonLeaderboardCount
  from counted
 where commonLeaderboardCount >= 3
),
songsThatWeMayLike as (
select SongHash,
       commonLeaderboardCount
  from ScoreSaberNeighbourScores Q1
  join playersThatLikeSongsWeDo Q2
    on Q1.PlayerID = Q2.PlayerID
),
foobar as (
  select SongHash,
         sum(commonLeaderboardCount) as commonLeaderboardCount,
         count(*) as PlayerCount
    from songsThatWeMayLike
group by SongHash
),
songsThatWeMayLikeThatWeDoNotAlreadyLike as (
   select Q1.SongHash,
          commonLeaderboardCount,
          PlayerCount
     from foobar Q1
left join favouriteSongs Q2
       on Q1.SongHash = Q2.SongHash
    where Q2.SongHash is NULL
      and PlayerCount >= 3
),
songsThatWeMayLikeThatWeHaveNotDownloaded as (
   select Q1.SongHash,
          commonLeaderboardCount,
          PlayerCount
     from songsThatWeMayLikeThatWeDoNotAlreadyLike Q1
left join DownloadedSongs Q2
       on Q1.SongHash = Q2.SongHash
    where Q2.SongHash is NULL
)
  select SongHash
    from songsThatWeMayLikeThatWeHaveNotDownloaded
order by commonLeaderboardCount DESC
EOF

  my $recordDownload = $dbh->prepare(<<'EOF');
INSERT OR REPLACE INTO DownloadedSongs (SongHash, SongDir, BeatSaverData, Deleted)
VALUES (?, ?, json(?), 0)
EOF

  $songsToDownload->execute();

  my $downloadedCount = 0;

  while (my ($songHash) = $songsToDownload->fetchrow_array()) {
    my $res;
    eval {
      $res = get("https://api.beatsaver.com/maps/hash/$songHash");
    };
    if ($EVAL_ERROR) {
      if ($EVAL_ERROR =~ m/404 Not Found/) {
        warn $EVAL_ERROR;
        next;
      }
      die $EVAL_ERROR;
    }

    my $data = decode_json($res);

    my $downloadURL;

    my $versions = $data->{versions};
    VERSION: foreach my $version (@{$versions}) {
      next unless $version->{state} eq "Published";
      next unless $songHash eq uc $version->{hash};
      $downloadURL = $version->{downloadURL};
      last VERSION;
    }

    next unless defined $downloadURL;

    my $id = $data->{id};
    my $songName = $data->{metadata}{songName};
    my $levelAuthorName = $data->{metadata}{levelAuthorName};
    my $songDirectory = "${id} (${songName} - ${levelAuthorName})";
    $songDirectory =~ s{[<>:/\\|?*"\x00-\x1f].*$}{};

    my $songPath = catdir($SongsFolder, $songDirectory);

    eval {
      my $zip = Archive::Zip->new();
      say "Downloading $downloadURL";

      my $zipData = get($downloadURL);
      open my $fh, "+<", \$zipData;
      $zip->readFromFileHandle($fh);

      say "Unpacking $downloadURL to $songPath";

      make_path $songPath;
      chdir $songPath;

      $zip->extractTree();
    };
    if ($EVAL_ERROR) {
      warn $EVAL_ERROR;
      next;
    }

    $recordDownload->execute($songHash, $songDirectory, $res);

    $downloadedCount++;
    last if $downloadedCount >= 10;
  }
}

sub createBackup {
  my $backupDir = catdir($DataDir, 'backup');

  make_path $backupDir;

  my $backupFile = strftime('%A.sqlite', localtime());

  my $backupPath = catfile($backupDir, $backupFile);

  $dbh->sqlite_backup_to_file($backupPath);
}

##############################################################

make_path $DataDir;

make_path $CacheDir;

open STDOUT, ">", catfile($DataDir, "nightly.txt");
STDOUT->autoflush(1);
STDOUT->binmode(':encoding(UTF-8)');
open STDERR, ">&STDOUT";

say "Starting run at ", ts();

fetchScores();

fetchLeaderboards();
pruneLeaderboards();

fetchNeighbours();
pruneNeighbours();

loadDownloadedSongs();
fetchBeatsaverData();
renameDownloadedSongs();

fetchNewSongs();

$dbh->do("PRAGMA incremental_vacuum");
$dbh->do("PRAGMA optimize");

createBackup();

say "Run complete at ", ts();
