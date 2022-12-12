#!/usr/bin/perl -CSDA

use warnings;
use strict;

use v5.16;
use utf8;

use Encode qw(encode decode);
use JSON qw(decode_json encode_json);
use Cwd qw(abs_path);
use DBI qw(SQL_NUMERIC);
use DBD::SQLite qw(SQLITE_UTF8 SQLITE_DETERMINISTIC);
use File::Spec::Functions qw(catdir catfile updir rel2abs);
use Carp;
use LWP::RobotUA;
use LWP::ConnCache;
use HTTP::Cache::Transparent;
use POSIX qw(ceil floor strftime round);
use Time::HiRes qw(time sleep);
use File::Path qw(make_path remove_tree);
use Digest::SHA;
use File::Slurp;
use Archive::Zip;
use IO::Compress::Gzip qw(gzip Z_BEST_COMPRESSION);
use File::stat;
use IPC::System::Simple qw(capturex);
use Time::Local qw(timelocal_modern);
use English qw(-no_match_vars);
use autodie qw(:all);
use autodie qw(encode decode decode_json encode_json abs_path catdir catfile updir rel2abs ceil floor strftime round time sleep gzip timelocal_modern);

$Carp::Verbose = 1;

my $PlayerID = '76561198001262880';

my $NOW = time();

my $SECONDS_PER_DAY = 24 * 60 * 60;
my $SECONDS_IN_15_MINUTES = 15 * 60;
my $SECONDS_IN_SIX_MONTHS = 6 * 30 * $SECONDS_PER_DAY;
my $SECONDS_PER_WEEK = $SECONDS_PER_DAY * 7;

my $DataDir = catdir($ENV{HOME}, ".BeatSaberScripts");

my $CacheDir = catdir($DataDir, "cache");

my $BeatSaberFolder = "/mnt/g/Steam/steamapps/common/Beat Saber";
my $BSLegacyLauncherFolder = "/mnt/g/Modding/Beat Saber/BSLegacyLauncher";
my $BeatSaberFolders = catdir($BSLegacyLauncherFolder, "Installed Versions");

my ($appDataFolder, $localAppDataFolder, $localLowAppDataFolder);
do {
  $appDataFolder = capturex("wslvar", "APPDATA");
  chomp($appDataFolder);
  $appDataFolder = capturex("wslpath", $appDataFolder);
  chomp($appDataFolder);

  $localAppDataFolder = capturex("wslvar", "LOCALAPPDATA");
  chomp($localAppDataFolder);
  $localAppDataFolder = capturex("wslpath", $localAppDataFolder);
  chomp($localAppDataFolder);

  $localLowAppDataFolder = abs_path(catdir($localAppDataFolder, updir(), "LocalLow"));
};

my $BeatSaberAppdataFolder = catdir($localLowAppDataFolder, "Hyperbolic Magnetism", "Beat Saber");

my $BeatSaviorDataAppdataFolder = catdir($appDataFolder,"Beat Savior Data");

my $YURFitAppdataFolder = catdir($localAppDataFolder, ".yurfit");

my $PlaylistFolder = catdir($BSLegacyLauncherFolder, "Playlists");

my $SongsFolder = catdir($BSLegacyLauncherFolder, "CustomLevels");

my $ACOUSTID_CLIENT_KEY = "wkf2hknaKU";

my $myAge = (time() - timelocal_modern(0, 0, 0, 18, 1, 1979)) / (60 * 60 * 24 * 365.2466);

my $maxHeartRate = 220 - $myAge;

my $BUILTIN_MAP_ACOUSTIDs = {
  BeatSaber => 'f98d851f-4f8a-4f5b-9d5a-35ee9136e605'
};

my $dbFile = catfile($DataDir, "data.sqlite");

my $dbh = DBI->connect(
  "dbi:SQLite:dbname=$dbFile","","",
  {
    sqlite_unicode => 1,
  }
);
$dbh->{HandleError} = sub { confess(shift) };

$dbh->do("PRAGMA auto_vacuum = INCREMENTAL");
$dbh->do("PRAGMA encoding = 'UTF-8'");
$dbh->do("PRAGMA foreign_keys = ON");
$dbh->do("PRAGMA journal_mode = WAL");
$dbh->do("PRAGMA busy_timeout = 250");
$dbh->do("PRAGMA optimize(0x4)");

#sub unhex { return pack('H*', $_[0]); }

#$dbh->sqlite_create_function('unhex', 1, \&unhex, SQLITE_UTF8|SQLITE_DETERMINISTIC);

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
  'Accept' => 'application/json',
  'Accept-Encoding' => scalar HTTP::Message::decodable()
);

my $ua2 = LWP::RobotUA->new(
  agent => 'buildBeatSaberPlaylist/0.1 ',
  from => 'Martin Rudat <mrudat@toraboka.com>',
  # allowed to make at most 3 requests/s
  delay => 1/(3 * 60),
  conn_cache => $connCache
);

$ua2->use_sleep(1);

$ua2->default_header(
  'Accept' => 'application/json',
  'Accept-Encoding' => scalar HTTP::Message::decodable()
);

$ua2->add_handler(
  request_prepare => sub {
    my ($request, $ua, $handler) = @_;

    my $uncompressed = $request->content;
    my $content;

    gzip(
      \$uncompressed => \$content,
      -Level => Z_BEST_COMPRESSION,
      Minimal => 1,
    );

    $request->content($content);
    $request->header( 'Content-Encoding' => 'gzip' );
    $request->header( 'Content-Length' => length($content) );
  },
  m_method => 'POST'
);

sub ts {
  return strftime("%Y-%m-%dT%H:%M:%S", localtime());
}

sub replaceInvalidCharacters {
  my ($data) = @_;
  $data =~ tr/<>:\/\\|?*"\x00-\x1f/‹›∶⁄⧵ǀ？∗″/d;
  return $data;
}

sub get2 {
  my ($url) = @_;

  my ($then) = time();

  my $res = $ua->get($url);
  my $elapsed = time() - $then;
  say "took ${elapsed}s, sleeping for ${elapsed}s";
  sleep($elapsed);

  unless ($res->is_success) {
    $res->content($res->decoded_content);
    croak $res->as_string;
  }

  return $res->decoded_content;
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

sub post2 {
  my ($url, $parameters) = @_;

  my ($then) = time();

  my $res = $ua2->post($url, $parameters);
  my $elapsed = time() - $then;
  say "took ${elapsed}s, sleeping for ${elapsed}s";
  sleep($elapsed);

  croak $res->as_string unless $res->is_success;

  return $res->decoded_content;
}

sub post {
  my ($url, $parameters) = @_;
  my $res;
  my $retry_count = 1;
  do {
    eval {
      $res = post2($url, $parameters);
    };
    if ($EVAL_ERROR) {
      if ($EVAL_ERROR =~ m{^HTTP/1.1 503}) {
        sleep(5 * $retry_count);
      } else {
        die $EVAL_ERROR;
      }
    }
    return $res;
  } while ($retry_count++ < 5);
  die $EVAL_ERROR;
}

sub create_or_replace_table {
  my ($sql) = @_;

  die "Can't determine table name" unless $sql =~ m/^\s*CREATE\s+TABLE\s+(\S+)/i;

  my $name = $1;

  # SQLite trims trailing whitespace.
  $sql =~ s/\s+$//s;

  my ($status) = $dbh->selectrow_array(<<'EOF', undef, $name, $sql);
select CASE
          WHEN sql = ?2 THEN 0
          ELSE 1
       END
  from sqlite_schema
 where type = 'table'
   and name = ?1
EOF

  if (defined $status && $status == 1) {
    $dbh->do("DROP TABLE $name");
  }

  if (!defined $status || $status == 1) {
    $dbh->do($sql);
  }
}

sub create_view {
  my ($sql) = @_;

  die "Can't determine view name" unless $sql =~ m/^\s*CREATE\s+VIEW\s+(\S+)/i;

  my $name = $1;

  # SQLite trims trailing whitespace.
  $sql =~ s/\s+$//s;

  my ($status) = $dbh->selectrow_array(<<'EOF', undef, $name, $sql);
select CASE
          WHEN sql = ?2 THEN 0
          ELSE 1
       END
  from sqlite_schema
 where type = 'view'
   and name = ?1
EOF

  if (defined $status && $status == 1) {
    $dbh->do("DROP VIEW $name");
  }

  if (!defined $status || $status == 1) {
    $dbh->do($sql);
  }
}

create_or_replace_table(<<'EOF');
CREATE TABLE Leaderboards (
  LeaderBoardID INTEGER PRIMARY KEY,
  SongHash TEXT,
  LevelID TEXT GENERATED ALWAYS AS ('custom_level_' || SongHash) VIRTUAL,
  GameMode TEXT,
  Difficulty INTEGER,
  MaxScore INTEGER,
  Data TEXT
)
EOF

$dbh->do(<<EOF);
CREATE INDEX IF NOT EXISTS Leaderboards_SongHash on Leaderboards (
  SongHash
)
EOF

create_or_replace_table(<<'EOF');
CREATE TABLE MyScoreSaberScores (
  ScoreID INTEGER PRIMARY KEY,
  LeaderBoardID INTEGER,
  BaseScore INTEGER,
  Rank INTEGER,
  TimeSet INTEGER,
  Data TEXT
)
EOF

$dbh->do(<<'EOF');
CREATE INDEX IF NOT EXISTS MyScoreSaberScores_LeaderboardID ON MyScoreSaberScores (
  LeaderboardID
)
EOF

sub fetchScores {
  my $baseUrl = "https://scoresaber.com/api/player/${PlayerID}/scores?sort=recent";

  my $res = get($baseUrl . "&withMetadata=true&limit=100");

  my $page = 1;

  my $meta = decode_json($res);

  my $itemsPerPage = $meta->{metadata}{itemsPerPage};

  $baseUrl .= "&withMetadata=false&limit=${itemsPerPage}";

  my $pages = ceil($meta->{metadata}{total} / $itemsPerPage);

  undef $meta;

  my $recordLeaderboard = $dbh->prepare(<<'EOF');
with playerScores as (
  SELECT value as playerScore
    from json_each(json(?), '$.playerScores')
)
insert or replace into Leaderboards (LeaderBoardID, SongHash, GameMode, Difficulty, MaxScore, Data)
select json_extract(playerScore, '$.leaderboard.id'),
       upper(json_extract(playerScore, '$.leaderboard.songHash')),
       json_extract(playerScore, '$.leaderboard.difficulty.gameMode'),
       json_extract(playerScore, '$.leaderboard.difficulty.difficulty'),
       json_extract(playerScore, '$.leaderboard.maxScore'),
       json_extract(playerScore, '$.leaderboard')
  from playerScores
EOF

  my $recordScore = $dbh->prepare(<<'EOF');
with playerScores as (
  SELECT value as playerScore
    from json_each(json(?), '$.playerScores')
)
insert or replace into MyScoreSaberScores (ScoreID, LeaderBoardID, BaseScore, Rank, TimeSet, Data)
select json_extract(playerScore, '$.score.id'),
       json_extract(playerScore, '$.leaderboard.id'),
       json_extract(playerScore, '$.score.baseScore'),
       json_extract(playerScore, '$.score.rank'),
       unixepoch(json_extract(playerScore, '$.score.timeSet')),
       json_extract(playerScore, '$.score')
  from playerScores
EOF

  while (1) {
    $recordLeaderboard->execute($res);
    $recordScore->execute($res);

    $page++;
    last if $page > $pages;

    $res = get($baseUrl . "&page=$page");
  }

  $dbh->do(<<EOF);
insert into Beatmaps (
  LevelId,
  GameMode,
  Difficulty,
  LeaderboardID,
  BestAccuracy,
  TopScorePlayed
)
with recentScores as (
select SongHash,
       CASE when GameMode like 'Solo%' then substr(GameMode, 5) else GameMode end as GameMode,
       Difficulty,
       Q1.LeaderboardID,
       julianday(TimeSet, 'unixepoch') as LastPlayed,
       1/(JULIANDAY('now') - JULIANDAY(TimeSet, 'unixepoch')) as Weight,
       BaseScore / cast(MaxScore as real) as Accuracy
  from MyScoreSaberScores Q1
  join Leaderboards Q2
    on Q1.LeaderboardID = Q2.LeaderboardID
 where MaxScore <> 0
   and BaseScore <> 0
)
select 'custom_level_' || upper(SongHash) as LevelID,
       GameMode,
       Difficulty,
       LeaderboardID,
       sum(Accuracy * Weight) / sum(Weight) as MyAccuracy,
       max(LastPlayed) as LastPlayed
  from recentScores
 group by SongHash,
          GameMode,
          Difficulty
on CONFLICT DO
UPDATE
   SET TopScorePlayed = excluded.TopScorePlayed,
       BestAccuracy = excluded.BestAccuracy,
       LeaderboardID = excluded.LeaderboardID
 where TopScorePlayed is null
    or TopScorePlayed <> excluded.TopScorePlayed
EOF
}

create_or_replace_table(<<'EOF');
CREATE TABLE TheirScoreSaberScores (
  ScoreID INTEGER PRIMARY KEY,
  PlayerID INTEGER,
  LeaderBoardID INTEGER,
  BaseScore INTEGER,
  TimeSet INTEGER,
  Data TEXT
)
EOF

$dbh->do(<<'EOF');
CREATE INDEX IF NOT EXISTS TheirScoreSaberScores_LeaderboardID ON TheirScoreSaberScores (
  LeaderboardID,
  PlayerID
)
EOF

$dbh->do(<<'EOF');
CREATE INDEX IF NOT EXISTS TheirScoreSaberScores_PlayerID ON TheirScoreSaberScores (
  PlayerID
)
EOF

create_view(<<'EOF');
CREATE VIEW NeighboursByScore AS
With scoreRatios as (
SELECT Q2.PlayerID,
       abs((Q2.BaseScore / cast(Q1.BaseScore as real)) - 1) as scoreRatio
  from MyScoreSaberScores Q1
  join TheirScoreSaberScores Q2
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
    where commonLeaderboardCount >= 3

EOF

sub fetchLeaderboards {
  my $recordScores = $dbh->prepare(<<'EOF');
with scores as (
  SELECT value as score
    from json_each(json(?1), '$.scores')
)
insert into TheirScoreSaberScores (ScoreID, PlayerID, LeaderBoardID, BaseScore, TimeSet, Data)
select json_extract(score, '$.id'),
       json_extract(score, '$.leaderboardPlayerInfo.id'),
       ?2,
       json_extract(score, '$.baseScore'),
       unixepoch(json_extract(score, '$.timeSet')),
       score
  from scores
 where json_extract(score, '$.leaderboardPlayerInfo.id') <> ?3
on conflict do
update
   set BaseScore = excluded.BaseScore,
       TimeSet = excluded.TimeSet,
       Data = excluded.Data
 where BaseScore <> excluded.BaseScore
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
    from MyScoreSaberScores
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
  }} while ($neighbourCount < 200 && @$nextLeaderboards);
}

sub fetchNeighbours {
  my $findNeighbours = $dbh->prepare(<<'EOF');
With scoreRatios as (
SELECT Q2.PlayerID,
       abs((Q2.BaseScore / cast(Q1.BaseScore as real)) - 1) as scoreRatio,
       Q2.TimeSet 
  from MyScoreSaberScores Q1
  join TheirScoreSaberScores Q2
    on Q1.LeaderboardID = Q2.LeaderboardID
),
counted as (
  SELECT PlayerID,
         count(*) as commonLeaderboardCount,
         max(TimeSet) as TimeSet
    from scoreRatios
   where scoreRatio < 0.1
group by PlayerID
)
   select PlayerID,
          commonLeaderboardCount
     from counted
    where commonLeaderboardCount >= 3
 order by commonLeaderboardCount DESC,
          TimeSet asc
EOF

  my $recordLeaderboard = $dbh->prepare(<<'EOF');
with playerScores as (
  SELECT value as playerScore
    from json_each(json(?), '$.playerScores')
)
insert or replace into Leaderboards (LeaderBoardID, SongHash, GameMode, Difficulty, MaxScore, Data)
select json_extract(playerScore, '$.leaderboard.id'),
       upper(json_extract(playerScore, '$.leaderboard.songHash')),
       json_extract(playerScore, '$.leaderboard.difficulty.gameMode'),
       json_extract(playerScore, '$.leaderboard.difficulty.difficulty'),
       json_extract(playerScore, '$.leaderboard.maxScore'),
       json_extract(playerScore, '$.leaderboard')
  from playerScores
EOF

  my $recordScore = $dbh->prepare(<<'EOF');
with playerScores as (
  SELECT value as playerScore
    from json_each(json(?1), '$.playerScores')
)
insert into TheirScoreSaberScores (ScoreID, PlayerID, LeaderBoardID, BaseScore, TimeSet, Data)
select json_extract(playerScore, '$.score.id'),
       ?2,
       json_extract(playerScore, '$.leaderboard.id'),
       json_extract(playerScore, '$.score.baseScore'),
       unixepoch(json_extract(playerScore, '$.score.timeSet')),
       json_extract(playerScore, '$.score')
  from playerScores
 where true
on conflict do
update
   set BaseScore = excluded.BaseScore,
       TimeSet = excluded.TimeSet,
       Data = excluded.Data
 where BaseScore <> excluded.BaseScore
EOF

  my $countPlayerScores = $dbh->prepare(<<'EOF');
select count(*)
  from TheirScoreSaberScores
 where PlayerID = ?
EOF

  $findNeighbours->execute();

  my $remaining = 200;

  while (my ($neighbourPlayerId, $count) = $findNeighbours->fetchrow_array()) {
    say "fetching scores for player $neighbourPlayerId with who we share $count leaderboards";

    my $baseUrl = "https://scoresaber.com/api/player/${neighbourPlayerId}/scores?sort=recent";

    my $res = get($baseUrl . "&withMetadata=true&limit=100");

    my $page = 1;

    my $meta = decode_json($res);

    my $itemsPerPage = $meta->{metadata}{itemsPerPage};
    my $totalScores = $meta->{metadata}{total};

    $baseUrl .= "&withMetadata=false&limit=${itemsPerPage}";

    my $pages = ceil($meta->{metadata}{total} / $itemsPerPage);

    say "$totalScores scores across $pages pages at $itemsPerPage per page.";

    undef $meta;

    my $multiplePages = 0;

    while (1) {
      $recordLeaderboard->execute($res);
      my ($updateCount) = $recordScore->execute($res, $neighbourPlayerId);

      if ($updateCount < $itemsPerPage) {
        $countPlayerScores->execute($neighbourPlayerId);
        my $playerScoreCount = $countPlayerScores->fetchrow_array();
        if ($playerScoreCount == $totalScores) {
          say "all scores for player $neighbourPlayerId are up-to-date";
          last;
        }
      }

      $page++;
      last if $page > $pages;

      $res = get($baseUrl . "&page=$page");
      $multiplePages = 1;
    }

    if ($multiplePages) {
      $remaining --;
      last if $remaining <= 0;
    }
  }
}

sub updatePredictedAccuracy {
  $dbh->do(<<'EOF');
insert into Beatmaps (
  LevelID,
  GameMode,
  Difficulty,
  PredictedAccuracy
)
with TheirAccuracy as (
select Q1.LeaderBoardID,
       Q2.PlayerID,
       MaxScore,
       (Q2.BaseScore * 1.0 / Q1.MaxScore) as TheirAccuracy
  from Leaderboards Q1
  join TheirScoreSaberScores Q2
    on Q1.LeaderBoardID = Q2.LeaderBoardID
 where MaxScore > 1000
   and BaseScore <> 0
   and json_extract(Q2.Data, '$.modifiers') in ('', 'NF', 'BE', 'IF')
),
Accuracy as (
select Q1.LeaderBoardID,
       PlayerID,
       Q2.MyAccuracy,
       TheirAccuracy
  from TheirAccuracy Q1
  join Beatmaps Q2
    on Q1.LeaderBoardID = Q2.LeaderBoardID 
 where MyAccuracy is not NULL
),
Similarity as (
  select PlayerID,
         1 - sqrt(avg(power(MyAccuracy - TheirAccuracy,2))) as Similarity,
         avg((MyAccuracy - TheirAccuracy)/((MyAccuracy + TheirAccuracy)/2)) as Similarity2,
         count(*) as SimilarScoreCount
    from Accuracy
group by PlayerID
  having count(*) >= 3
),
TheirAccuracyCount as (
  select PlayerID,
         count(*) as TotalScoreCount
    from TheirAccuracy
group by PlayerID
),
Combined as (
      select PlayerID,
             Similarity * SimilarScoreCount / TotalScoreCount as Weight,
             Similarity2
        from Similarity
natural join TheirAccuracyCount
),
PredictedAccuracy as (
      select LeaderBoardID,
             sum(TheirAccuracy * Weight) / sum(Weight) as PredictedAccuracy,
             avg(Similarity2) as AvgSimilarity2
        from TheirAccuracy
natural join Combined
       where Weight > 0.1
    group by LeaderBoardID
      having count(*) >= 3
         and avg(Similarity2) >= -0.02
)
select LevelID,
       substr(GameMode,5) as GameMode,
       Difficulty,
       PredictedAccuracy
  from Leaderboards Q1
  join PredictedAccuracy Q2
    on Q1.LeaderBoardID = Q2.LeaderBoardID
 where GameMode like 'Solo%'
on CONFLICT DO
UPDATE
   SET PredictedAccuracy = excluded.PredictedAccuracy
EOF
}

sub updateProjectedAccuracy {
  # project new score from trends of changing score over time
  $dbh->do(<<EOF);
with a as (
  select Q2.LeaderBoardID,
         FileDate,
         Accuracy
    from BeatSaviourData Q1
    join Leaderboards Q2
      on Q1.SongID = Q2.SongHash
     and 'Solo' || coalesce(Q1.GameMode, 'Standard') = Q2.GameMode
     and Q1.SongDifficultyRank = Q2.Difficulty
   where FileDate > JULIANDAY('now') - 365/2
),
b as (
  select LeaderboardID
    from a
group by LeaderboardID
  having count(*) > 1
),
filtered as (
      select a.*
        from a
natural join b
),
banded as (
select *,
       ntile(10) over (order by Accuracy) as band
  from filtered
),
MinBand as (
  select LeaderboardID,
         round(avg(band)) as Band
    from banded
group by LeaderboardID
),
Foo as (
select LeaderboardID,
       FileDate,
       AVG(FileDate) OVER (PARTITION BY LeaderboardID) as AverageFileDate,
       Accuracy,
       AVG(Accuracy) OVER (PARTITION BY LeaderboardID) as AverageAccuracy
  from Filtered
),
Slope as (
  select LeaderboardID,
         SUM((FileDate - AverageFileDate) * (Accuracy - AverageAccuracy)) / SUM((FileDate - AverageFileDate) * (FileDate - AverageFileDate)) as slope,
         count(*) as PlayCount
    from Foo
group by LeaderboardID
),
AverageSlope as (
      Select Band,
             avg(slope) as AccuracyPerDay,
             sqrt(avg(slope*slope)) as StdDev,
             sum(PlayCount) as PlayCount
        from Slope
natural join MinBand
    group by Band
),
BandRanges as (
  select Band,
         min(Accuracy) as AverageAccuracy
    from banded
group by Band
),
AdjustedAccuracyPerDay as (
      select AverageAccuracy,
             max(AccuracyPerDay,min(AccuracyPerDay + StdDev,0.0001/365)) AdjustedAccuracyPerDay
        from BandRanges
natural join AverageSlope
),
PlayTimes as (
select rowid as BeatmapID,
       coalesce(MyAccuracy,BestAccuracy) as Accuracy,
       max(coalesce(LastPlayed,0), coalesce(TopScorePlayed,0), coalesce(YURFitDataTime,0)) as LastPlayed
  from Beatmaps
),
FilteredPlayTimes as (
select BeatmapID,
       Accuracy,
       LastPlayed
  from PlayTimes
 where Accuracy is not null
   and LastPlayed is not null
),
TwoColumnSelectExpressionPart1 as (
select BeatmapID,
       Accuracy,
       LastPlayed,
       (
  select AverageAccuracy
    from AdjustedAccuracyPerDay
   where Accuracy >= AverageAccuracy
order by AverageAccuracy desc
   limit 1
       ) as AverageAccuracyLow,
       (
  select AverageAccuracy
    from AdjustedAccuracyPerDay
   where Accuracy <= AverageAccuracy
order by AverageAccuracy asc
   limit 1
       ) as AverageAccuracyHigh
   from FilteredPlayTimes
),
TwoColumnSelectExpressionPart2 as (
select BeatmapID,
       Accuracy,
       LastPlayed,
       AverageAccuracyLow,
       (
  select AdjustedAccuracyPerDay
    from AdjustedAccuracyPerDay
   where AverageAccuracy = AverageAccuracyLow
       ) as AdjustedAccuracyPerDayLow,
       AverageAccuracyHigh,
       (
  select AdjustedAccuracyPerDay
    from AdjustedAccuracyPerDay
   where AverageAccuracy = AverageAccuracyHigh
       ) as AdjustedAccuracyPerDayHigh
  from TwoColumnSelectExpressionPart1
),
LinearInterpolation as (
select BeatmapID,
       Accuracy,
       LastPlayed,
       CASE
         WHEN AverageAccuracyLow is NULL
           THEN AdjustedAccuracyPerDayHigh
         WHEN AverageAccuracyHigh is NULL
           THEN AdjustedAccuracyPerDayLow
         ELSE (
                AdjustedAccuracyPerDayLow * (AverageAccuracyHigh - Accuracy)
              + AdjustedAccuracyPerDayHigh * (Accuracy - AverageAccuracyLow)
              ) / (AverageAccuracyHigh - AverageAccuracyLow)
       END as AdjustedAccuracyPerDay
  from TwoColumnSelectExpressionPart2
),
ProjectedAccuracy as (
select BeatmapID,
       Accuracy,
       LastPlayed,
       Accuracy + ((julianday('now') - LastPlayed) * AdjustedAccuracyPerDay) as ProjectedAccuracy
  from LinearInterpolation
)
Update Beatmaps as Q1
   set ProjectedAccuracy = Q2.ProjectedAccuracy
  from ProjectedAccuracy as Q2
 where Q1.rowid = Q2.BeatmapID
EOF
}

sub pruneNeighbours {
  $dbh->do(<<'EOF');
With myScores as (
SELECT LeaderboardID,
       BaseScore as myScore
  from MyScoreSaberScores
),
neighbourScores as (
SELECT PlayerID,
       LeaderboardID,
       BaseScore as neighbourScore
  from TheirScoreSaberScores
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
),
potentialVictims as (
  select PlayerID,
         count(*)
    from TheirScoreSaberScores
   where PlayerID not in (
                    select PlayerID
                      from potentialNeighbours
                         )
group by PlayerID
  having count(*) > 1
)
delete 
  from TheirScoreSaberScores
 where PlayerID in (
              select PlayerID
                from potentialVictims
                   )
EOF
}

$dbh->do(<<'EOF');
CREATE TABLE IF NOT EXISTS DownloadedSongs (
  SongHash TEXT PRIMARY KEY,
  SongDir TEXT,
  BeatSaverData TEXT,
  Deleted INTEGER,
  Duration INTEGER GENERATED ALWAYS AS (json_extract(BeatSaverData, '$.metadata.duration')) virtual,
  FingerprintID INTEGER
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

  my $digest = Digest::SHA->new(1);

  opendir(my $dh, $SongsFolder);

  use Data::Dumper;

  while (my $name = readdir $dh) {
    next if $name =~ m/^\./;
    next if $name =~ m/^$/;

    my $songDir = catdir($SongsFolder, $name);

    next unless -d $songDir;

    my $infoFile = catfile($songDir, 'Info.dat');

    next unless -f $infoFile;

    $name = decode('UTF-8', $name);

    $getHaveHash->execute($name);
    my ($haveHash) = $getHaveHash->fetchrow_array();

    if ($haveHash) {
      $recordSeen->execute($name);
      next;
    }

    # TODO produce 300x300 cover for later?
    # TODO perform mergeSongFiles now?

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

    my $keptDir;

    foreach my $songDir (@{$songDirs}) {
      my $songPath = catdir($SongsFolder, encode('UTF-8', $songDir));
      next unless -d $songPath;
      if (!defined $keptDir) {
        $keptDir = $songDir;
        say "keeping $songDir";
        next;
      }
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
   and not Deleted
)
select SongDir, MapName
  from MapNames
 where SongDir <> MapName
EOF

  $namesToFix->execute();
  while (my ($songDir, $mapName) = $namesToFix->fetchrow_array()) {
    $mapName = replaceInvalidCharacters($mapName);
    next if $songDir eq $mapName;
    say "renaming $songDir to $mapName";
    $songDir = encode('UTF-8', $songDir);
    $mapName = encode('UTF-8', $mapName);
    my $oldPath = catdir($SongsFolder, $songDir);
    my $tempPath = catdir($SongsFolder, "$mapName-tmp");
    my $newPath = catdir($SongsFolder, $mapName);
    eval {
      rename($oldPath, $tempPath);
      rename($tempPath, $newPath);
    };
    if ($EVAL_ERROR) {
      warn $EVAL_ERROR;
    }
  }
}

sub fetchNewSongs {
  my $songsToDownload = $dbh->prepare(<<'EOF');
  select Hash
    from Levels
   where Hash is not null
     and LikeFactor is not null
     and ( IsDownloaded is NULL or IsDownloaded = FALSE )
     and LevelID in (
    select LevelID
      from Beatmaps
     where PredictedAccuracy > 0.6
     )
order by LikeFactor desc
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

    my $duration = $data->{metadata}{duration};
    if ($duration <= 90) {
      say "Not downloading short song ($duration < 90)";
      next;
    }

    my $id = $data->{id};
    my $songName = $data->{metadata}{songName};
    my $levelAuthorName = $data->{metadata}{levelAuthorName};
    my $songDirectory = "${id} (${songName} - ${levelAuthorName})";
    $songDirectory = replaceInvalidCharacters($songDirectory);

    my $songPath = catdir($SongsFolder, encode('UTF-8', $songDirectory));

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

  $dbh->do(<<EOF);
insert into Levels (
  LevelId,
  IsDownloaded,
  IsDeleted,
  Duration
)
select 'custom_level_' || upper(SongHash) as LevelId,
       TRUE,
       Deleted,
       Duration
  from DownloadedSongs
 where true
on CONFLICT DO
UPDATE
   SET IsDownloaded = excluded.IsDownloaded,
       IsDeleted = excluded.IsDeleted,
       Duration = excluded.Duration
EOF
}

sub mergeSongFiles {
  my ($dotMusicFolder) = catdir($SongsFolder, ".music");
  opendir(my $songsDh, $SongsFolder);
  while (my $songDirName = readdir($songsDh)) {
    next if $songDirName =~ m/^\./;
    my $songFolder = catdir($SongsFolder, $songDirName);
    next unless -d $songFolder;
    opendir(my $songDh, $songFolder);
    while (my $filename = readdir($songDh)) {
      next if $filename =~ m/^\./;

      next unless $filename =~ m/\.(egg)$/;

      my $file = catfile($songFolder, $filename);

      my $st = stat $file;

      next unless -f $st;

      next if $st->nlink > 1;

      my $hash = capturex("ffmpeg", "-i", $file, "-map", "0:a", "-c", "copy", "-f", "md5", "-");
      chomp($hash);
      next unless $hash =~ s/^MD5=//;
      # TODO remember hash for later
      # TODO build 300x300 image, store in music directory
      # TODO rebuild image from cover embedded in song file if updated.
      # TODO read Info.dat

      my $musicPath = catdir($dotMusicFolder, $hash);
      my $musicFile = catfile($musicPath, "song.ogg");
      my $infoPath = catfile($songFolder, "Info.dat");
      my $musicInfoPath = catdir($musicPath, "$songDirName.dat");

      say "$file -> $musicFile";
      say "$infoPath -> $musicInfoPath";

      my $tmpFile = "$file.tmp";
      my $tmpMusicInfoPath = "$musicInfoPath.tmp";

      make_path($musicPath);

      link($infoPath, $tmpMusicInfoPath);
      rename($tmpMusicInfoPath, $musicInfoPath);

      my $index;
      my $targetMusicFile = $musicFile;
      while (-f $targetMusicFile) {
        $index++;
        $targetMusicFile = catfile($musicPath, "song ($index).ogg");
      }
      say "Saving another copy in case it has better metadata." if ($index);
      rename($file, $targetMusicFile);
      link($musicFile, $tmpFile);
      rename($tmpFile, $file);

    }
    closedir $songDh;
  }
  closedir $songsDh;

  opendir(my $musicDh, $dotMusicFolder);
  while(my $dirname = readdir $musicDh) {
    next if $dirname =~ m/^\./;

    my $dir = catdir($dotMusicFolder, $dirname);

    my $file = catfile($dir, "song.ogg");

    my $st = stat $file;

    next if $st->nlink > 1;

    say "removing $dirname as no levels reference it.";

    remove_tree($dir);
  }

  closedir $musicDh;
}

$dbh->do(<<'EOF');
CREATE TABLE IF NOT EXISTS AcoustIDs (
  ID INTEGER PRIMARY KEY,
  Fingerprint TEXT,
  AcoustID TEXT,
  LastFetchDate REAL
)
EOF

$dbh->do(<<'EOF');
CREATE UNIQUE INDEX IF NOT EXISTS AcoustIDs_AK ON AcoustIDs (
  Fingerprint
)
EOF

sub getAcoustIDs {
  my $needFingerprint = $dbh->prepare(<<EOF);
select rowid, SongDir
  from DownloadedSongs
 where FingerprintID is NULL
   and not Deleted
EOF

  my $registerFingerprint = $dbh->prepare(<<EOF);
insert into AcoustIDs (Fingerprint)
values (json(?1))
on conflict do
update
   set LastFetchDate = LastFetchDate 
returning ID,
          AcoustID is not null
EOF

  my $recordFingerprint = $dbh->prepare(<<EOF);
update DownloadedSongs
   set FingerprintID = ?2
 where rowid = ?1
EOF

  my $recordAcoustID = $dbh->prepare(<<EOF);
update AcoustIDs
  set AcoustID = json(?2),
      LastFetchDate = julianday('now')
where ID = ?1
EOF

  my $recordFailure = $dbh->prepare(<<EOF);
update AcoustIDs
  set LastFetchDate = julianday('now')
where ID = ?1
EOF

  $needFingerprint->execute();

  while(my ($rowid, $songDirName) = $needFingerprint->fetchrow_array()) {
    my ($songDir) = catdir($SongsFolder, encode('UTF-8', $songDirName));

    next unless -d $songDir;

    my ($infoFile) = catfile($songDir, 'Info.dat');

    next unless -f $infoFile;

    my ($info) = decode_json(read_file($infoFile));

    my ($songFileName) = $info->{_songFilename};

    my ($songFile) = catfile($songDir, encode('UTF-8', $songFileName));

    next unless -f $songFile;

    my ($displaySongFile) = catfile(catdir($SongsFolder, $songDirName), $songFileName);
    say "Calculating fingerprint of $displaySongFile";

    my ($fingerprintJson) = capturex("fpcalc", "-json", $songFile);

    unless (defined $fingerprintJson && length($fingerprintJson) > 0) {
      say "FIXME No output. Is fpcalc having a nap?";
      next;
    };

    my $fingerprint = decode_json($fingerprintJson);

    $registerFingerprint->execute($fingerprintJson);

    my ($fingerprintID, $hasAcoustID) = $registerFingerprint->fetchrow_array();

    $recordFingerprint->execute($rowid, $fingerprintID);

    next if $hasAcoustID;

    say "Fetching AcoustID for $displaySongFile";

    my $resJson = eval {
      post('https://api.acoustid.org/v2/lookup', [
        client => $ACOUSTID_CLIENT_KEY,
        duration => round($fingerprint->{duration}),
        fingerprint => $fingerprint->{fingerprint},
        meta => 'recordingids'
      ]);
    };
    if ($EVAL_ERROR) {
      if ($EVAL_ERROR =~ m{^HTTP/1.1 500}) {
        warn $EVAL_ERROR;
        $recordFailure->execute($fingerprintID);
        next;
      } else {
        die $EVAL_ERROR;
      }
    }

    my $res = decode_json($resJson);

    if ($res->{status} ne "ok") {
      warn $res->{message};
      $recordFailure->execute($fingerprintID);
      next;
    }

    $recordAcoustID->execute($fingerprintID, $resJson);
  }

  my $needAcoustId = $dbh->prepare(<<'EOF');
select ID,
       Fingerprint
  from AcoustIDs
 where AcoustID is null
    or (
         json_array_length(AcoustID, '$.results') = 0
     and LastFetchDate < JULIANDAY('now') - 30
       )
EOF

  $needAcoustId->execute();

  while(my ($fingerprintID, $fingerprintJson) = $needAcoustId->fetchrow_array()) {
    my $fingerprint = decode_json($fingerprintJson);

    my $resJson = eval {
      post('https://api.acoustid.org/v2/lookup', [
        client => $ACOUSTID_CLIENT_KEY,
        duration => round($fingerprint->{duration}),
        fingerprint => $fingerprint->{fingerprint},
        meta => 'recordingids'
      ]);
    };
    if ($EVAL_ERROR) {
      if ($EVAL_ERROR =~ m{^HTTP/1.1 500}) {
        warn $EVAL_ERROR;
        $recordFailure->execute($fingerprintID);
        next;
      } else {
        die $EVAL_ERROR;
      }
    }

    my $res = decode_json($resJson);

    if ($res->{status} ne "ok") {
      warn $res->{message};
      $recordFailure->execute($fingerprintID);
      next;
    }

    $recordAcoustID->execute($fingerprintID, $resJson);
  }
}

$dbh->do(<<EOF);
CREATE TABLE IF NOT EXISTS PlayerData (
  ID integer primary key,
  PlayerData TEXT
)
EOF

create_view(<<EOF);
CREATE VIEW FavouriteLevels AS
select value as LevelId
  from PlayerData,
       json_each(PlayerData, '\$.localPlayers[0].favoritesLevelIds')
EOF

sub loadFavourites {
  my $playerDataFile = catfile($BeatSaberAppdataFolder, "PlayerData.dat");

  return unless -f $playerDataFile;

  say "Reading $playerDataFile at ", ts();

  $dbh->do("insert or replace into PlayerData (id, PlayerData) values (1, json(?))", undef, read_file($playerDataFile));

  $dbh->do(<<EOF);
Insert into Levels (
  LevelId,
  IsFavourite
)
SELECT LevelId,
       TRUE
  FROM FavouriteLevels
 where true
on CONFLICT DO
UPDATE
   SET IsFavourite = TRUE
EOF

  $dbh->do(<<EOF);
update Levels
   set IsFavourite = FALSE
 where LevelID not in (
  select LevelID
    from FavouriteLevels
 )
EOF
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

  $dbh->do(<<EOF);
insert into Levels (
  LevelId,
  Vote
)
select 'custom_level_' || upper(SongHash) as LevelId,
       Case VoteType
         WHEN 'Upvote' then 1
         WHEN 'Downvote' then -1
       END as Vote
  from votedSongs
 where true
on CONFLICT DO
UPDATE
   SET Vote = excluded.Vote
EOF
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

sub markBeatmapsBanned {
  $dbh->do(<<EOF);
insert into Beatmaps (
  LevelID,
  GameMode,
  Difficulty,
  IsBanned
)
with combined as (
   select coalesce(Q1.LevelID, Q2.LevelID) as LevelID,
          coalesce(Q1.GameMode, Q2.GameMode) as GameMode,
          coalesce(
            CASE Q1.Difficulty
              WHEN 'Easy' THEN 1
              WHEN 'Normal' THEN 3
              WHEN 'Hard' THEN 5
              WHEN 'Expert' THEN 7
              WHEN 'ExpertPlus' THEN 9
              ELSE Q1.Difficulty
            END,
            Q2.Difficulty
          ) as Difficulty,
          TRUE as IsBanned
     from BannedSongs Q1
     join Beatmaps Q2
       on Q1.LevelID = Q2.LevelID 
      and Q1.GameMode = Q2.GameMode
      and Q1.Difficulty = Q2.DifficultyText
)
select LevelId,
       GameMode,
       Difficulty,
       IsBanned
  from combined
 where true
on CONFLICT DO
UPDATE
   SET IsBanned = excluded.IsBanned
 where IsBanned is null
    or IsBanned <> excluded.IsBanned
EOF

  $dbh->do(<<EOF);
insert into Beatmaps (
  LevelID,
  GameMode,
  Difficulty,
  IsBanned
)
with combined as (
   select coalesce(Q1.LevelID, Q2.LevelID) as LevelID,
          coalesce(Q1.GameMode, Q2.GameMode) as GameMode,
          coalesce(
            CASE Q1.Difficulty
              WHEN 'Easy' THEN 1
              WHEN 'Normal' THEN 3
              WHEN 'Hard' THEN 5
              WHEN 'Expert' THEN 7
              WHEN 'ExpertPlus' THEN 9
              ELSE Q1.Difficulty
            END,
            Q2.Difficulty
          ) as Difficulty,
          FALSE as IsBanned
     from Beatmaps Q2
left join BannedSongs Q1
       on Q1.LevelID = Q2.LevelID 
      and Q1.GameMode = Q2.GameMode
      and Q1.Difficulty = Q2.DifficultyText
    where Q2.IsBanned
      and Q1.LevelID is NULL
)
select LevelId,
       GameMode,
       Difficulty,
       IsBanned
  from combined
 where true
on CONFLICT DO
UPDATE
   SET IsBanned = excluded.IsBanned
 where IsBanned is null
    or IsBanned <> excluded.IsBanned
EOF
}

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

  if ($lastModified && $lastModified == $stat->mtime) {
    markBeatmapsBanned();
    return;
  };

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
select distinct json_extract(song, '$.levelid') as levelid,
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

  markBeatmapsBanned();

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

create_or_replace_table(<<'EOF');
CREATE TABLE DuplicateSongs (
  FileName TEXT PRIMARY KEY,
  LastModifiedTime INT,
  LevelIDs TEXT,
  Seen INT
)
EOF

sub loadDuplicateSongs {
  my $duplicatesFolder = catdir($PlaylistFolder, 'Duplicates');

  my $getLastModified = $dbh->prepare(<<EOF);
   update DuplicateSongs
      set Seen = TRUE
    where FileName = ?
returning LastModifiedTime
EOF

  my $recordDuplicates = $dbh->prepare(<<'EOF');
insert or replace into DuplicateSongs (FileName, LastModifiedTime, LevelIDs, Seen)
with songs as (
select value as song
  from json_each(json(?3), '$.songs')
),
levelIDs as (
select json_extract(song, '$.levelid') as LevelID
  from songs
)
select ?1,
       ?2,
       json_group_array(distinct LevelID),
       TRUE
  from levelIDs
EOF

  $dbh->do(<<EOF);
update DuplicateSongs
   set Seen = FALSE
EOF

  opendir(my $dh, $duplicatesFolder);

  while (my $file = readdir $dh) {
    next unless $file =~ m/\.bplist$/;
    my $duplicatesFile = catfile($duplicatesFolder, $file);

    my $st = stat $duplicatesFile;

    $getLastModified->execute($file);

    my ($lastModifiedTime) = $getLastModified->fetchrow_array();

    next if $lastModifiedTime && $lastModifiedTime == $st->mtime;

    my $data = read_file($duplicatesFile);

    $recordDuplicates->execute($file, $st->mtime, $data);
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
  LeftHandDistance INTEGER GENERATED ALWAYS AS (json_extract(Data, '$.trackers.distanceTracker.leftHand')) VIRTUAL,
  RightHandDistance INTEGER GENERATED ALWAYS AS (json_extract(Data, '$.trackers.distanceTracker.rightHand')) VIRTUAL,
  LeftSaberDistance INTEGER GENERATED ALWAYS AS (json_extract(Data, '$.trackers.distanceTracker.leftSaber')) VIRTUAL,
  RightSaberDistance INTEGER GENERATED ALWAYS AS (json_extract(Data, '$.trackers.distanceTracker.rightSaber')) VIRTUAL,
  SongDuration INTEGER GENERATED ALWAYS AS (json_extract(Data, '$.songDuration')) VIRTUAL,
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

  my ($updateBeatmaps) = 0;

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

    $updateBeatmaps = 1;

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

  if (!$updateBeatmaps) {
    my ($hasData) = $dbh->selectrow_array(<<EOF);
select TRUE
  from Beatmaps
 where LastPlayed is not null
 limit 1
EOF
    $updateBeatmaps = !$hasData;
  }

  if ($updateBeatmaps) {
    $dbh->do(<<EOF);
insert into Beatmaps (
  LevelId,
  GameMode,
  Difficulty,
  MyAccuracy,
  SaberSpeed,
  HandSpeed,
  Duration,
  LastPlayed
)
with recentScores as (
select SongId as SongHash,
       CASE
         -- TODO extract version number. BeatSaviorData 2.1.13+ returns the incorrect gameMode
         WHEN FileDate >= 2459883.5 THEN 'Standard'
         ELSE COALESCE(GameMode, 'Standard') 
       END as GameMode,
       SongDifficultyRank as Difficulty,
       FileDate as LastPlayed,
       1/(JULIANDAY('now') - FileDate) as Weight,
       Accuracy,
       LeftSaberDistance + RightSaberDistance as SaberDistance,
       LeftHandDistance + RightHandDistance as HandDistance,
       SongDuration
  from BeatSaviourData
)
select CASE
         WHEN length(SongHash) <> 40
           or length(trim(SongHash,'0123456789ABCDEF')) <> 0
           THEN SongHash
         ELSE 'custom_level_' || upper(SongHash)
       END as LevelId,
       GameMode,
       Difficulty,
       sum(Accuracy * Weight) / sum(Weight) as MyAccuracy,
       sum(SaberDistance * Weight / SongDuration) / sum(Weight) as SaberSpeed,
       sum(HandDistance * Weight / SongDuration) / sum(Weight) as HandSpeed,
       sum(SongDuration * Weight) / sum(Weight) as Duration,
       max(LastPlayed) as LastPlayed
  from recentScores
 group by SongHash,
          GameMode,
          Difficulty
on CONFLICT DO
UPDATE
   SET LastPlayed = excluded.LastPlayed,
       MyAccuracy = excluded.MyAccuracy,
       SaberSpeed = excluded.SaberSpeed,
       HandSpeed  = excluded.HandSpeed,
       Duration   = excluded.Duration
 where LastPlayed is null
    or LastPlayed <> excluded.LastPlayed
EOF
  }
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

  $dbh->do(<<'EOF');
CREATE TABLE IF NOT EXISTS YURFitData (
  StartTime DATE,
  SongHash TEXT,
  GameMode TEXT,
  Difficulty TEXT,
  MaximumRatio NUMBER,
  AnaerobicRatio NUMBER,
  AerobicRatio NUMBER,
  WeightControlRatio NUMBER,
  LowIntensityRatio NUMBER,
  PRIMARY KEY(StartTime, SongHash, GameMode, Difficulty)
)
EOF

$dbh->do(<<EOF);
CREATE TABLE IF NOT EXISTS YURFitFiles (
  FileDate DATE primary key,
  LastModified TIMESTAMP
)
EOF

sub loadYURFitData {
  $dbh->do(<<'EOF');
CREATE TEMPORARY TABLE YURFitTemp (
  FileDate DATE,
  StartTime DATE,
  SongName TEXT,
  Difficulty TEXT,
  MaximumRatio NUMBER,
  AnaerobicRatio NUMBER,
  AerobicRatio NUMBER,
  WeightControlRatio NUMBER,
  LowIntensityRatio NUMBER,
  PRIMARY KEY(StartTime, SongName, Difficulty)
)
EOF

  my $getLastModified = $dbh->prepare(<<'EOF');
select LastModified
  from YURFitFiles
 where FileDate = julianday(?)
EOF

  my $saveLastModified = $dbh->prepare(<<'EOF');
insert or replace into YURFitFiles (FileDate, LastModified)
values (julianday(?), ?)
EOF

  my $recordBPM = $dbh->prepare(<<'EOF');
INSERT OR REPLACE INTO YURFitTemp (
  FileDate,
  StartTime,
  SongName,
  Difficulty,
  MaximumRatio,
  AnaerobicRatio,
  AerobicRatio,
  WeightControlRatio,
  LowIntensityRatio
)
Values (
  julianday(?1),
  julianday(?2, 'unixepoch'),
  ?3,
  CASE
    WHEN ?4 = 'Expert+' THEN 'ExpertPlus'
    ELSE ?4
  END,
  ?5,
  ?6,
  ?7,
  ?8,
  ?9
)
EOF

  my $logDir = catdir($YURFitAppdataFolder, "logs");

  opendir(my $dh, $logDir);

  my $updateBeatmaps = 0;

  while (my $name = readdir $dh) {
    next unless $name =~ m/^YUR\.Fit\.Windows\.Service-(\d\d\d\d)(\d\d)(\d\d)\.txt/;
    my ($fileDate) = "$1-$2-$3";

    my $filePath = catfile($logDir, $name);

    my $st = stat $filePath;

    next unless $st && -f $st;

    my $lastModified = $st->mtime;

    $getLastModified->execute($fileDate);
    my $oldLastModified = $getLastModified->fetchrow_array();

    next if $oldLastModified && $oldLastModified == $lastModified;

    $updateBeatmaps = 1;

    say "Reading $filePath at ", ts();

    open my $fh, "<", $filePath;

    my ($data, $thing);

    while (my $line = <$fh>) {
      if ($line =~ m/^(\d\d\d\d)-(\d\d)-(\d\d)T(\d\d):(\d\d):(\d\d\.\d\d\d\d\d\d)\d[+-]\d\d:\d\d  \[INF\] Set workout metadata to (.+) \([0-9a-f]+\)/) {
        my ($year, $month, $day, $hour, $minute, $seconds) = ($1, $2, $3, $4, $5, $6);
        $thing = $7;
        my $ts = timelocal_modern($seconds, $minute, $hour, $day, $month - 1, $year);
        push @{$data->{$thing}}, [ $ts ];
      }
      if ($line =~ m/^(\d\d\d\d)-(\d\d)-(\d\d)T(\d\d):(\d\d):(\d\d\.\d\d\d\d\d\d)\d[+-]\d\d:\d\d  \[INF\] Got heart rate: (\d+)/) {
        if ($thing) {
          my ($year, $month, $day, $hour, $minute, $seconds, $bpm) = ($1, $2, $3, $4, $5, $6, $7);
          my $ts = timelocal_modern($seconds, $minute, $hour, $day, $month - 1, $year);
          push @{$data->{$thing}[-1][1]}, [ $bpm, $ts ];
        }
      }
    }

    close $fh;

    $dbh->begin_work();

    foreach $thing (keys %{$data}) {
      next if $thing eq "Main Menu";

      next unless $thing =~ m/^(.*) - (.*)$/;
      my ($songName, $difficulty) = ($1, $2);

      foreach my $data2 (@{$data->{$thing}}) {
        my $startTime = $data2->[0];
        my $lastTime = $startTime;
        my $data3 = $data2->[1];

        my ($maximumTime, $anaerobicTime, $aerobicTime, $weightControlTime, $lowIntensityTime) = (0, 0, 0, 0, 0);

        my $totalTime;

        foreach my $foo (@{$data3}) {
          my ($bpm, $ts) = @$foo;
          my $maxBpmFraction = $bpm / $maxHeartRate;
          my $duration = $lastTime - $ts;

          # classify bpm by intensity.
          # 90+ Max
          # 80+ Anaerobic
          # 70+ Aerobic
          # 60+ Weight Control
          # 50+ Low Intensitory

          if ($maxBpmFraction > 0.9) {
            $maximumTime += $duration;
          } elsif ($maxBpmFraction > 0.8) {
            $anaerobicTime += $duration;
          } elsif ($maxBpmFraction > 0.7) {
            $aerobicTime += $duration;
          } elsif ($maxBpmFraction > 0.6) {
            $weightControlTime += $duration;
          } elsif ($maxBpmFraction > 0.5) {
            $lowIntensityTime += $duration;
          }

          $totalTime += $duration;

          $lastTime = $ts;
        }

        next unless defined $totalTime;
        
        $recordBPM->execute(
          $fileDate,
          $startTime,
          $songName,
          $difficulty,
          $maximumTime / $totalTime,
          $anaerobicTime / $totalTime,
          $aerobicTime / $totalTime,
          $weightControlTime / $totalTime,
          $lowIntensityTime / $totalTime
        );
      }
    }

    $dbh->do(<<'EOF');
insert or replace into YURFitData (
  StartTime,
  SongHash,
  GameMode,
  Difficulty,
  MaximumRatio,
  AnaerobicRatio,
  AerobicRatio,
  WeightControlRatio,
  LowIntensityRatio
)
select StartTime,
       SongId,
       coalesce(GameMode, 'Standard'),
       SongDifficultyRank,
       MaximumRatio,
       AnaerobicRatio,
       AerobicRatio,
       WeightControlRatio,
       LowIntensityRatio
  from YURFitTemp Q1
  join BeatSaviourData Q2
    on Q1.FileDate = Q2.FileDate 
   and Q1.SongName = json_extract(Data, '$.songName')
   and lower(Q1.Difficulty) = json_extract(Data, '$.songDifficulty')
EOF

    $saveLastModified->execute($fileDate, $lastModified);

    $dbh->commit();
  }
  closedir $dh;

  if (!$updateBeatmaps) {
    my ($hasData) = $dbh->selectrow_array(<<EOF);
select TRUE
  from Beatmaps
 where YURFitDataTime is not null
 limit 1
EOF
    $updateBeatmaps = !$hasData;
  }

  if ($updateBeatmaps) {
    $dbh->do(<<EOF);
insert into Beatmaps (
  LevelID,
  GameMode,
  Difficulty,
  YURFitDataTime,
  MaximumRatio,
  AnaerobicRatio,
  AerobicRatio,
  WeightControlRatio,
  LowIntensityRatio
)
with YURFitDataWeighted as (
Select SongHash,
       GameMode,
       Difficulty,
       StartTime,
       MaximumRatio,
       AnaerobicRatio,
       AerobicRatio,
       WeightControlRatio,
       LowIntensityRatio,
       1/(julianday('now') - StartTime) as Weight
  from YURFitData
),
YURFitDataSummed as (
  Select SongHash,
         GameMode,
         Difficulty,
         max(StartTime) as StartTime,
         sum(MaximumRatio * Weight) as TotalMaximumRatio,
         sum(AnaerobicRatio * Weight) as TotalAnaerobicRatio,
         sum(AerobicRatio * Weight) as TotalAerobicRatio,
         sum(WeightControlRatio * Weight) as TotalWeightControlRatio,
         sum(LowIntensityRatio * Weight) as TotalLowIntensityRatio,
         sum(Weight) as TotalWeight
  from YURFitDataWeighted
group by SongHash,
         GameMode,
         Difficulty
)
Select 'custom_level_' || upper(SongHash) as LevelID,
       GameMode,
       Difficulty,
       StartTime,
       TotalMaximumRatio / TotalWeight as MaximumRatio,
       TotalAnaerobicRatio / TotalWeight as AnaerobicRatio,
       TotalAerobicRatio / TotalWeight as AerobicRatio,
       TotalWeightControlRatio / TotalWeight as WeightControlRatio,
       TotalLowIntensityRatio / TotalWeight as LowIntensityRatio
  from YURFitDataSummed
 where true
on CONFLICT DO
UPDATE
   SET YURFitDataTime = excluded.YURFitDataTime,
       MaximumRatio = excluded.MaximumRatio,
       AnaerobicRatio = excluded.AnaerobicRatio,
       AerobicRatio = excluded.AerobicRatio,
       WeightControlRatio = excluded.WeightControlRatio,
       LowIntensityRatio = excluded.LowIntensityRatio
 where YURFitDataTime is null
    or YURFitDataTime <> excluded.YURFitDataTime
EOF
  }

}

sub pruneYURFitData {
  $dbh->do(<<'EOF');
WITH Numbered AS (
Select ROWID,
       ROW_NUMBER() OVER (PARTITION BY SongHash, GameMode, Difficulty order by StartTime desc) as ROW_NUM
  from YURFitData Q1
)
DELETE FROM YURFitData
 WHERE ROWID IN (
          select ROWID
            from Numbered
           where row_num > 30
                )
EOF

  $dbh->do(<<EOF);
with Numbered as (
select ROWID,
       ROW_NUMBER() over (order by fileDate desc) as row_num
  from YURFitFiles
)
delete from YURFitFiles
 where ROWID in (
           select ROWID
             from Numbered
            where row_num > 31
                )
EOF
}

create_or_replace_table(<<EOF);
CREATE TABLE Songs (
  ID INT primary key,
  LastPlayed DATE,
  IsFavourite INT,
  RecentPlayCount INT
)
EOF

create_or_replace_table(<<EOF);
CREATE TABLE Levels (
  LevelID TEXT CHECK (CASE WHEN LevelID LIKE 'custom_level_%' THEN substr(LevelID,14) = upper(substr(LevelID,14)) ELSE TRUE END) primary key not null,
  Hash TEXT GENERATED ALWAYS AS (
    CASE
      WHEN LevelID LIKE 'custom_level_%'
        THEN substr(LevelID,14)
      ELSE NULL
    END
  ) VIRTUAL,
  SongID INT,
  Duration INT,
  IsFavourite INT DEFAULT FALSE,
  Vote INT DEFAULT 0,
  IsDownloaded INT,
  IsDeleted INT,
  LikeFactor REAL
)
EOF

$dbh->do(<<'EOF');
CREATE INDEX IF NOT EXISTS Levels_CanPlay ON Levels (
  IsDownloaded,
  IsDeleted
)
EOF

$dbh->do(<<'EOF');
CREATE INDEX IF NOT EXISTS Levels_SongID ON Levels (
  SongID
)
EOF

create_or_replace_table(<<EOF);
CREATE TABLE Beatmaps (
  LevelID TEXT CHECK (CASE WHEN LevelID LIKE 'custom_level_%' THEN substr(LevelID,14) = upper(substr(LevelID,14)) ELSE TRUE END),
  Hash TEXT GENERATED ALWAYS AS (
    CASE
      WHEN LevelID LIKE 'custom_level_%'
        THEN substr(LevelID,14)
      ELSE NULL
    END
  ) VIRTUAL,
  GameMode INT,
  Difficulty INT,
  DifficultyText TEXT GENERATED ALWAYS AS (
    CASE Difficulty
      WHEN 1 THEN 'Easy'
      WHEN 3 THEN 'Normal'
      WHEN 5 THEN 'Hard'
      WHEN 7 THEN 'Expert'
      WHEN 9 THEN 'ExpertPlus'
      ELSE Difficulty
    END
  ) VIRTUAL,
  LeaderboardID INT,
  LastPlayed DATE,
  TopScorePlayed DATE,
  YURFitDataTime DATE,
  MaximumRatio REAL,
  AnaerobicRatio REAL,
  AerobicRatio REAL,
  WeightControlRatio REAL,
  LowIntensityRatio REAL,
  BestAccuracy REAL,
  MyAccuracy REAL,
  PredictedAccuracy REAL,
  ProjectedAccuracy REAL,
  IsBanned INT,
  LikeFactor REAL,
  SaberSpeed REAL,
  HandSpeed REAL,
  Duration REAL,
  PRIMARY KEY (LevelID, GameMode, Difficulty)
)
EOF

$dbh->do(<<'EOF');
CREATE INDEX IF NOT EXISTS Beatmaps_IsBanned ON Beatmaps (
  IsBanned
)
EOF

sub updatePotentiallyLikedSongs {
  $dbh->do(<<EOF);
insert into Levels (
  LevelId,
  LikeFactor
)
with favouriteLevels as (
select Hash as SongHash
  from Songs s
  join Levels l
    on s.ID = l.SongID 
 where s.IsFavourite
   and Hash is not null
),
myFavouriteLeaderboards as (
   SELECT Q1.SongHash,
          Q2.LeaderboardID
     from favouriteLevels Q1
left join Leaderboards Q2
       on Q1.SongHash = Q2.SongHash
),
deletedLevels AS (
SELECT LeaderboardID
  FROM DownloadedSongs Q1
  join Leaderboards Q2
    on Q1.SongHash = Q2.SongHash
 WHERE Deleted
),
neighbourLeaderboards as (
SELECT PlayerID,
       LeaderboardID
  from TheirScoreSaberScores
),
commonLeaderboards as (
SELECT Q2.PlayerID
  from myFavouriteLeaderboards Q1
  join neighbourLeaderboards Q2
    on Q1.LeaderboardID = Q2.LeaderboardID
),
deletedCommonLeaderboards as (
SELECT Q2.PlayerID
  from deletedLevels Q1
  join neighbourLeaderboards Q2
    on Q1.LeaderboardID = Q2.LeaderboardID
),
commonLeaderboardCount as (
  SELECT PlayerID,
         count(*) as commonLeaderboardCount
    from commonLeaderboards
group by PlayerID
),
deletedLeaderboardCount as (
  SELECT PlayerID,
         count(*) as deletedLeaderboardCount
    from deletedCommonLeaderboards
group by PlayerID
),
playersThatLikeSongsWeDo as (
   select Q1.PlayerID,
          commonLeaderboardCount - COALESCE(deletedLeaderboardCount,0) as commonLeaderboardCount
     from commonLeaderboardCount Q1
left join deletedLeaderboardCount Q2
       on Q1.PlayerID = Q2.PlayerID
    where (commonLeaderboardCount - COALESCE(deletedLeaderboardCount,0)) >= 3
),
levels2 as (
select distinct SongHash,
       PlayerID
  from TheirScoreSaberScores Q1
  join Leaderboards Q2
    on Q1.LeaderBoardID = Q2.LeaderBoardID 
),
levelsThatWeMayLike as (
select SongHash,
       commonLeaderboardCount
  from levels2 Q1
  join playersThatLikeSongsWeDo Q2
    on Q1.PlayerID = Q2.PlayerID
),
distinctLevelsThatWeMayLike as (
  select SongHash,
         sum(commonLeaderboardCount) as commonLeaderboardCountSum,
         count(*) as PlayerCount
    from levelsThatWeMayLike
group by SongHash
)
select 'custom_level_' || upper(SongHash) as LevelId,
       commonLeaderboardCountSum * 1.0 / PlayerCount as likeFactor
  from distinctLevelsThatWeMayLike
 where PlayerCount >= 3
on CONFLICT DO
UPDATE
   SET LikeFactor = excluded.LikeFactor
EOF

  $dbh->do(<<EOF);
insert into Beatmaps (
  LevelID,
  GameMode,
  Difficulty,
  LikeFactor
)
with favouriteSongs as (
select substr(LevelId,14) as SongHash
  from FavouriteLevels
 where LevelId LIKE 'custom_level_%'
),
myFavouriteLeaderboards as (
SELECT LeaderboardID
  from Leaderboards Q1
  join FavouriteSongs Q2
    on Q1.SongHash = Q2.SongHash
),
neighbourLeaderboards as (
SELECT PlayerID,
       LeaderboardID
  from TheirScoreSaberScores
),
commonLeaderboards as (
SELECT Q2.PlayerID
  from myFavouriteLeaderboards Q1
  join neighbourLeaderboards Q2
    on Q1.LeaderboardID = Q2.LeaderboardID
),
commonLeaderboardCount as (
  SELECT PlayerID,
         count(*) as commonLeaderboardCount
    from commonLeaderboards
group by PlayerID
),
playersThatLikeSongsWeDo as (
select PlayerID,
       commonLeaderboardCount
  from commonLeaderboardCount
 where commonLeaderboardCount >= 3
),
beatmapsThatWeMayLike as (
select SongHash,
       GameMode,
       Difficulty,
       commonLeaderboardCount
  from Leaderboards Q1
  join TheirScoreSaberScores Q2
    on Q1.LeaderBoardID = Q2.LeaderBoardID 
  join playersThatLikeSongsWeDo Q3
    on Q2.PlayerID = Q3.PlayerID
),
distinctBeatmapsThatWeMayLike as (
  select SongHash,
         GameMode,
         Difficulty,
         sum(commonLeaderBoardCount) as commonLeaderBoardCountSum,
         count(*) as PlayerCount
    from beatmapsThatWeMayLike
group by SongHash,
         GameMode,
         Difficulty
),
likeFactor as (
select 'custom_level_' || upper(SongHash) as LevelId,
       CASE 
         WHEN GameMode LIKE 'Solo%'
           THEN substr(GameMode, 5)
         ELSE GameMode
       END as GameMode,
       Difficulty,
       commonLeaderBoardCountSum * 1.0 / PlayerCount as LikeFactor
  from distinctBeatmapsThatWeMayLike
 where PlayerCount >= 3
)
select LevelId,
       GameMode,
       Difficulty,
       LikeFactor
  from likeFactor
 where true
on CONFLICT DO
UPDATE
   SET LikeFactor = excluded.LikeFactor
EOF
}

sub identifyDuplicateSongs {
  say "merging duplicate songs at ", ts();

  $dbh->do(<<EOF);
update Levels
   set SongID = rowid
EOF

  $dbh->do(<<EOF);
with a as (
select Q1.rowid,
       SongID,
       FingerprintID
  from Levels Q1
  join DownloadedSongs Q2
    on Q1.Hash = Q2.SongHash
 where FingerprintID is not null
),
MinSongID as (
  select FingerprintID,
         min(SongID) as MinSongID
    from a
group by FingerPrintID
  having count(*) > 1
)
update Levels as Q3
   set SongID = MinSongID
  from (
  select MinSongID, rowid
    from MinSongID Q1
    join a Q2
      on Q1.FingerprintID = Q2.FingerprintID
   where SongID <> MinSongID
       ) as Q4
 where Q3.rowid = Q4.rowid
EOF

  my ($mergeAcoustIDs) = $dbh->prepare(<<'EOF');
with IDs as (
select SongID,
       json_extract (results.value, '$.id') acoustID
  from Levels Q1
  join DownloadedSongs Q2
    on Q1.Hash = Q2.SongHash
  join AcoustIDs Q3
    on Q2.FingerprintID = Q3.ID,
       json_each(json_extract(Q3.AcoustID, '$.results')) results
),
duplicate as (
  select acoustID
    from IDs
group by acoustID
  having count(distinct songID) > 1
order by count(distinct songID) desc
   limit 1
),
songs as (
select SongID
  from IDs Q1
  join duplicate Q2
    on Q1.acoustID = Q2.acoustID
),
MinSongID as (
select min(SongID) as MinSongID
  from songs
)
update Levels as Q1
   set SongID = MinSongID
  from (
  select MinSongID,
         SongID
    from MinSongID,
         songs
   where SongID <> MinSongID
       ) as Q2
 where Q1.SongID = Q2.SongID
EOF

  while ($mergeAcoustIDs->execute() > 0) { };

  my ($mergeRecordingIDs) = $dbh->prepare(<<'EOF');
with IDs as (
select SongID,
       json_extract (results.value, '$.id') acoustID,
       json_extract (recordings.value, '$.id') recordingID
  from Levels Q1
  join DownloadedSongs Q2
    on Q1.Hash = Q2.SongHash
  join AcoustIDs Q3
    on Q2.FingerprintID = Q3.ID,
       json_each(json_extract(Q3.AcoustID, '$.results')) results,
       json_each(json_extract(results.value, '$.recordings')) recordings
),
duplicate as (
  select recordingID
    from IDs
group by recordingID
  having count(distinct songID) > 1
order by count(distinct songID) desc
   limit 1
),
songs as (
select SongID
  from IDs Q1
  join duplicate Q2
    on Q1.recordingID = Q2.recordingID
),
MinSongID as (
select min(SongID) as MinSongID
  from songs
)
update Levels as Q1
   set SongID = MinSongID
  from (
  select MinSongID,
         SongID
    from MinSongID,
         songs
   where SongID <> MinSongID
       ) as Q2
 where Q1.SongID = Q2.SongID
EOF

  while ($mergeRecordingIDs->execute() > 0) { };

  my ($mergeDuplicateSongs) = $dbh->prepare(<<'EOF');
with duplicates as (
select ds.rowid as DuplicateID,
       foo.value as LevelID
  from DuplicateSongs ds,
       json_each(json(LevelIDs)) foo
),
foo as (
select Q2.DuplicateID,
       Q1.SongID
  from Levels Q1
  join duplicates Q2
    on Q1.LevelID = Q2.LevelID
),
MinSongID as (
  select DuplicateID,
         min(SongID) as MinSongID
    from foo
group by DuplicateID
  having count(distinct SongID) > 1
order by count(distinct songID) desc
   limit 1
)
update Levels as Q1
   set SongID = MinSongID
  from (
  select MinSongID,
         SongID
    from MinSongID Q1
    join foo Q2
      on Q1.DuplicateID = Q2.DuplicateID
   where SongID <> MinSongID
       ) as Q2
 where Q1.SongID = Q2.SongID
EOF

  while ($mergeDuplicateSongs->execute() > 0) { };

  say "finished merging duplicate songs at ", ts();
}

sub updateSongStats {
  $dbh->do(<<EOF);
insert or replace into Songs (ID, LastPlayed, IsFavourite)
With sigh as (
  select LevelID,
         max(
           coalesce(LastPlayed, -1),
           coalesce(TopScorePlayed, -1),
           coalesce(YURFitDataTime, -1)
         ) as LastPlayed
    from Beatmaps
),
LastPlayed as (
select LevelID,
       Case LastPlayed 
         WHEN -1 THEN NULL
         ELSE LastPlayed
       end as LastPlayed
  from sigh
)
  select SongID,
         max(LastPlayed) as LastPlayed,
         max(IsFavourite) as IsFavourite
    from Levels Q1
    join LastPlayed Q2
      on Q1.LevelID = Q2.LevelID 
group by SongID
EOF

  $dbh->do(<<EOF);
with a as (
  select Q2.SongID,
         count(*) as bsdCount
    from BeatSaviourData Q1
    join Levels Q2
      on Q1.SongId = Q2.Hash
   where FileDate >= (JULIANDAY('now') - 30)
group by Q2.SongID
),
b as (
  select Q2.SongID,
         count(*) as yurCount
    from YURFitData Q1
    join Levels Q2
      on Q1.SongHash = Q2.Hash 
   where StartTime > JULIANDAY('now') - 30
group by Q2.SongID
)
update Songs as Q1
   set RecentPlayCount = PlayCount
  from (
  select a.SongID,
         max(
           coalesce(bsdCount, 0),
           coalesce(yurCount, 0)
         ) as PlayCount
    from a
    join b
      on a.SongID = b.SongID
       ) as Q2
 where Q1.ID = Q2.SongID
EOF

  $dbh->do(<<EOF);
delete from Songs
 where ID in (
        select Q1.ID
          from Songs Q1
     left join Levels Q2
            on Q1.ID = Q2.SongID
         where Q2.SongID is null
             )
EOF
}

sub writeSongsWithoutBeatSaviourStats {
  my $data = $dbh->selectall_arrayref(<<'EOF', undef, $0)->[0][0];
with weWantToPlayThisAgain as (
  select Q2.SongID,
         Q1.LevelID,
         GameMode,
         DifficultyText,
         coalesce(MyAccuracy, BestAccuracy) as LastScore
    from Beatmaps Q1
    join Levels Q2
      on Q1.LevelID = Q2.LevelID
    join Songs Q3
      on Q2.SongID = Q3.ID
   where IsDownloaded
     and (IsDeleted is NULL or IsDeleted = FALSE)
     and TopScorePlayed is not null
     and (
           Q1.LastPlayed is null
        or YURFitDataTime is null
         )
     and (RecentPlayCount is NULL or RecentPlayCount < 10)
     and GameMode not in ('Lightshow', 'OneSaber', '360Degree')
),
RowNum as (
select Q1.*,
       ROW_NUMBER() OVER (partition by SongID order by LastScore desc) as RowNum
  from weWantToPlayThisAgain Q1
)
select json_object(
         'image',
         '',
         'playlistAuthor',
         ?1,
         'playlistTitle',
         'Play Again',
         'songs',
         json_group_array(
           json_object(
             'levelid',
             LevelID,
             'lastScore',
             LastScore,
             'difficulties',
             json_array(
               json_object(
                 'characteristic', GameMode,
                 'name', DifficultyText
               )
             )
           )
         )
       )
  from RowNum
 where RowNum = 1
order by LastScore desc
EOF

  my $target = catfile($PlaylistFolder, 'play-again.bplist');

  if (index($data,'[]') > 0) {
    unlink $target if (-f $target);
    return;
  }

  write_file($target, $data);
}

sub writeSongsToImprove {
  write_file(catfile($PlaylistFolder, 'to-improve.bplist'), $dbh->selectall_arrayref(<<'EOF', undef, $0)->[0][0]);
with toImprove as (
  select Q1.LevelID,
         GameMode,
         DifficultyText,
         MyAccuracy,
         ProjectedAccuracy
    from Beatmaps Q1
    join Levels Q2
      on Q1.LevelID = Q2.LevelID
   where ProjectedAccuracy > myAccuracy
     and IsDownloaded
     and (IsDeleted is NULL or IsDeleted = FALSE)
     and GameMode not in ('Lightshow', 'OneSaber', '360Degree')
order by ProjectedAccuracy / myAccuracy desc
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
             'levelid',
             LevelID,
             'difficulties',
             json_array(
               json_object(
                 'characteristic', GameMode,
                 'name', DifficultyText
               )
             ),
             'MyAccuracy',
             MyAccuracy,
             'ProjectedAccuracy',
             ProjectedAccuracy
           )
         )
       )
  from toImprove
EOF
}

sub writeNotPlayedSongs {
  write_file(catfile($PlaylistFolder, 'not-played.bplist'), $dbh->selectall_arrayref(<<'EOF', undef, $0)->[0][0]);
with ranked as (
select SongID,
       Q1.LevelID,
       GameMode,
       DifficultyText,
       PredictedAccuracy,
       percent_rank() over (order by predictedAccuracy desc) as accuracyRank,
       Q1.LikeFactor,
       PERCENT_RANK() over (order by Q2.IsFavourite asc, Q1.LikeFactor asc) as likeRank
  from Beatmaps Q1
  join Levels Q2
    on Q1.LevelID = Q2.LevelID
  join Songs Q3
    on Q2.SongID = Q3.ID
 where IsDownloaded
   and (IsDeleted is NULL or IsDeleted = FALSE)
   and PredictedAccuracy is not null
   and Q1.LikeFactor is not null
   and Q1.LastPlayed is null
   and (RecentPlayCount is NULL or RecentPlayCount < 10)
   and GameMode not in ('Lightshow', 'OneSaber', '360Degree')
   and PredictedAccuracy >= 0.8
),
newSongs as (
  select SongID,
         LevelID,
         GameMode,
         DifficultyText,
         PredictedAccuracy,
         LikeFactor,
         (accuracyRank + 0.5)
       * (likeRank + 0.5) as CombinedOrder
    from ranked
),
RowNum as (
select Q1.*,
       ROW_NUMBER() OVER (partition by SongID order by CombinedOrder desc) as RowNum
  from newSongs Q1
order by CombinedOrder desc
)
select json_object(
         'image',
         '',
         'playlistAuthor',
         ?1,
         'playlistTitle',
         'Not Played Easy',
         'songs',
         json_group_array(
           json_object(
             'levelid',
             LevelID ,
             'difficulties',
             json_array(
               json_object(
                 'characteristic', GameMode,
                 'name', DifficultyText
               )
             ),
             'predictedAccuracy',
             PredictedAccuracy,
             'likeFactor',
             LikeFactor
           )
         )
       )
  from RowNum
 where RowNum = 1
EOF

  write_file(catfile($PlaylistFolder, 'not-played-hard.bplist'), $dbh->selectall_arrayref(<<'EOF', undef, $0)->[0][0]);
with ranked as (
select SongID,
       Q1.LevelID,
       GameMode,
       DifficultyText,
       PredictedAccuracy,
       percent_rank() over (order by predictedAccuracy desc) as accuracyRank,
       Q1.LikeFactor,
       PERCENT_RANK() over (order by Q2.IsFavourite asc, Q1.LikeFactor asc) as likeRank
  from Beatmaps Q1
  join Levels Q2
    on Q1.LevelID = Q2.LevelID
  join Songs Q3
    on Q2.SongID = Q3.ID
 where IsDownloaded
   and (IsDeleted is NULL or IsDeleted = FALSE)
   and PredictedAccuracy is not null
   and Q1.LikeFactor is not null
   and Q1.LastPlayed is null
   and (RecentPlayCount is NULL or RecentPlayCount < 10)
   and GameMode not in ('Lightshow', 'OneSaber', '360Degree')
   and PredictedAccuracy BETWEEN 0.6 AND 0.8
),
newSongs as (
  select SongID,
         LevelID,
         GameMode,
         DifficultyText,
         PredictedAccuracy,
         LikeFactor,
         (accuracyRank + 0.5)
       * (likeRank + 0.5) as CombinedOrder
    from ranked
),
RowNum as (
select Q1.*,
       ROW_NUMBER() OVER (partition by SongID order by CombinedOrder desc) as RowNum
  from newSongs Q1
order by CombinedOrder desc
)
select json_object(
         'image',
         '',
         'playlistAuthor',
         ?1,
         'playlistTitle',
         'Not Played Hard',
         'songs',
         json_group_array(
           json_object(
             'levelid',
             LevelID ,
             'difficulties',
             json_array(
               json_object(
                 'characteristic', GameMode,
                 'name', DifficultyText
               )
             ),
             'predictedAccuracy',
             PredictedAccuracy,
             'likeFactor',
             LikeFactor
           )
         )
       )
  from RowNum
 where RowNum = 1
EOF
}

create_view(<<'EOF');
CREATE VIEW WorkoutPlan AS
with
combined as (
   select Q3.ID as SongID,
          Q1.LevelID,
          Difficulty,
          GameMode,
          MyAccuracy,
          PredictedAccuracy,
          ProjectedAccuracy,
          julianday('now') - julianday(max(Q1.LastPlayed,TopScorePlayed,YURFitDataTime)) as BeatmapAge,
          julianday('now') - julianday(Q3.LastPlayed) as SongAge,
          SaberSpeed,
          HandSpeed,
          Q2.IsFavourite,
          Q1.Duration,
          Vote,
          MaximumRatio,
          AnaerobicRatio,
          AerobicRatio,
          WeightControlRatio,
          LowIntensityRatio
     from Beatmaps Q1
     join Levels Q2
       on Q1.LevelID = Q2.LevelID
     join Songs Q3
       on Q2.SongID = Q3.ID
    where IsDownloaded
      and (IsDeleted is NULL or IsDeleted = FALSE)
      and (IsBanned is NULL or IsBanned = FALSE)
      and (RecentPlayCount is null or RecentPlayCount < 5)
      and PredictedAccuracy >= 0.6
      and (ProjectedAccuracy is null or ProjectedAccuracy >= 0.6)
),
calculations as (
select SongID,
       LevelID,
       Difficulty,
       GameMode,
       CASE
         WHEN PredictedAccuracy >= MyAccuracy
           THEN PredictedAccuracy / MyAccuracy
         ELSE NULL
       END as PotentialImprovement,
       ProjectedAccuracy,
       CASE
         WHEN ProjectedAccuracy > MyAccuracy
           THEN ProjectedAccuracy / MyAccuracy
         ELSE NULL
       END as PotentialImprovement2,
       PredictedAccuracy as PotentialScore,
       BeatmapAge,
       SongAge,
       HandSpeed / SaberSpeed as WristFactor,
       HandSpeed,
       SaberSpeed,
       IsFavourite,
       Duration,
       abs((3 * 60) - Duration) as OffsetFromDesiredTime,
       Vote,
       MaximumRatio,
       AnaerobicRatio,
       AerobicRatio,
       WeightControlRatio,
       LowIntensityRatio
  from combined
 where (BeatmapAge is NULL or BeatmapAge > 7)
   and (SongAge is NULL or SongAge > 3)
),
averages as (
select avg(WristFactor) as AverageWristFactor,
       avg(HandSpeed) as AverageHandSpeed
  from calculations
)
select SongID,
       LevelID,
       Difficulty,
       GameMode,
       PotentialImprovement,
       percent_rank() over (order by PotentialImprovement asc nulls first) as ImprovementRank,
       ProjectedAccuracy,
       PotentialImprovement2,
       percent_rank() over (order by PotentialImprovement2 asc nulls first) as Improvement2Rank,
       PotentialScore,
       percent_rank() over (order by PotentialScore asc nulls first) as PotentialScoreRank,
       BeatmapAge,
       percent_rank() over (order by BeatmapAge asc nulls last) as BeatmapAgeRank,
       SongAge,
       percent_rank() over (order by SongAge asc nulls last) as SongAgeRank,
       WristFactor,
       percent_rank() over (order by COALESCE(WristFactor, AverageWristFactor) asc) as WristFactorRank,
       HandSpeed,
       percent_rank() over (order by COALESCE(HandSpeed, AverageHandSpeed) asc) as HandSpeedRank,
       SaberSpeed,
       IsFavourite,
       Duration,
       percent_rank() over (order by OffsetFromDesiredTime desc) as DurationRank,
       Vote,
       (Vote + 2) / 3.0 as VoteRank,
       MaximumRatio,
       AnaerobicRatio,
       AerobicRatio,
       WeightControlRatio,
       LowIntensityRatio,
       percent_rank() over (order by AerobicRatio asc nulls first) as WorkoutRank,
       percent_rank() over (order by WeightControlRatio asc nulls first) as Workout2Rank
  from calculations,
       averages
EOF

sub writeWorkout {
  my $targetWorkoutDuration = 40 * 60;

  write_file(catfile($PlaylistFolder, 'workout.bplist'), $dbh->selectall_arrayref(<<EOF, undef, $0)->[0][0]);
with CombinedOrder as (
select Q1.*,
       (ImprovementRank + 0.5)
       * (Improvement2Rank + 0.5)
       * (WorkoutRank + 0.5)
       * (IsFavourite + 1)
       * (VoteRank)
       * (DurationRank + 0.5) as CombinedOrder
    from WorkoutPlan Q1
   where BeatmapAge is NOT NULL
     and GameMode not in ('Lightshow', 'OneSaber', '360Degree')
     and ProjectedAccuracy BETWEEN 0.6 and 0.8
),
RowNum as (
select Q1.*,
       ROW_NUMBER() OVER (partition by SongID order by CombinedOrder desc) as RowNum
  from CombinedOrder Q1
),
TotalDuration as (
  select Q1.*,
         sum(Duration) over (order by CombinedOrder desc rows between UNBOUNDED PRECEDING and CURRENT ROW) TotalDuration
    from RowNum Q1
   where RowNum = 1
order by CombinedOrder desc
),
SpeedOrder as (
  select Q1.*,
         ROW_NUMBER() over (order by SaberSpeed asc) as SpeedOrder
    from TotalDuration Q1
   where TotalDuration <= $targetWorkoutDuration
),
FinalOrder as (
  select LevelID,
         GameMode,
         CASE Difficulty
           WHEN 1 THEN 'Easy'
           WHEN 3 THEN 'Normal'
           WHEN 5 THEN 'Hard'
           WHEN 7 THEN 'Expert'
           WHEN 9 THEN 'ExpertPlus'
         ELSE Difficulty
       END as Difficulty 
    from SpeedOrder Q1
order by case SpeedOrder 
           WHEN 1
             THEN NULL
           ELSE SpeedOrder
         end asc nulls last
)
select json_object(
         'image',
         '',
         'playlistAuthor',
         ?1,
         'playlistTitle',
         'Workout Hard',
         'songs',
         json_group_array(
           json_object(
             'levelid',
             LevelID,
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
  from FinalOrder
EOF
}

sub writeWorkout2 {
  my $targetWorkoutDuration = 40 * 60;

  write_file(catfile($PlaylistFolder, 'workout2.bplist'), $dbh->selectall_arrayref(<<EOF, undef, $0)->[0][0]);
with CombinedOrder as (
select Q1.*,
       (ImprovementRank + 0.5)
       * (Improvement2Rank + 0.5)
       * (Workout2Rank + 0.5)
       * (IsFavourite + 1)
       * (VoteRank)
       * (DurationRank + 0.5) as CombinedOrder
    from WorkoutPlan Q1
   where BeatmapAge is NOT NULL
     and GameMode not in ('Lightshow', 'OneSaber', '360Degree')
     and Q1.ProjectedAccuracy >= 0.8
),
RowNum as (
select Q1.*,
       ROW_NUMBER() OVER (partition by SongID order by CombinedOrder desc) as RowNum
  from CombinedOrder Q1
),
TotalDuration as (
  select Q1.*,
         sum(Duration) over (order by CombinedOrder desc rows between UNBOUNDED PRECEDING and CURRENT ROW) TotalDuration
    from RowNum Q1
   where RowNum = 1
order by CombinedOrder desc
),
SpeedOrder as (
  select Q1.*,
         ROW_NUMBER() over (order by SaberSpeed asc) as SpeedOrder
    from TotalDuration Q1
   where TotalDuration <= $targetWorkoutDuration
),
FinalOrder as (
  select LevelID,
         GameMode,
         CASE Difficulty
           WHEN 1 THEN 'Easy'
           WHEN 3 THEN 'Normal'
           WHEN 5 THEN 'Hard'
           WHEN 7 THEN 'Expert'
           WHEN 9 THEN 'ExpertPlus'
         ELSE Difficulty
       END as Difficulty 
    from SpeedOrder Q1
order by case SpeedOrder 
           WHEN 1
             THEN NULL
           ELSE SpeedOrder
         end asc nulls last
)
select json_object(
         'image',
         '',
         'playlistAuthor',
         ?1,
         'playlistTitle',
         'Workout Easy',
         'songs',
         json_group_array(
           json_object(
             'levelid',
             LevelID,
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
  from FinalOrder
EOF
}

sub updateSettings {
  my $getEnableLeftHanded = $dbh->prepare(<<'EOF');
select sum(LeftHandDistance) - sum(RightHandDistance)
  from BeatSaviourData
 where FileDate >= julianday('now') - 7
EOF

  $getEnableLeftHanded->execute();

  my ($enableLeftHanded) = $getEnableLeftHanded->fetchrow_array();

  if ($enableLeftHanded > 0) {
    say "left hand has moved ", $enableLeftHanded, "m more than right hand over the last week.";
  } else {
    say "right hand has moved ", -$enableLeftHanded, "m more than left hand over the last week.";
  }

  my $playerDataFile = catfile($BeatSaberAppdataFolder, "PlayerData.dat");
  my $playerData = read_file($playerDataFile);

  my ($newPlayerData) = @{$dbh->selectall_arrayref(<<'EOF', undef, $playerData, $enableLeftHanded < 0 ? 'true' : 'false')};
select json_replace(
         json(?1),
         '$.localPlayers[0].playerSpecificSettings.leftHanded',
         json(?2)
       )
EOF

  if ($newPlayerData ne $playerData) {
    write_file($playerDataFile, $newPlayerData);
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

my ($runType) = $ARGV[0];

if (defined $runType) {
  my $logFileName = catfile($DataDir, "${runType}.txt");

  my $st = stat $logFileName;
  if ($st && $st->size >= 1048576) {
    rename($logFileName, catfile($DataDir, "${runType}-prev.txt"));
  }
  
  open STDOUT, ">>:encoding(utf8)", $logFileName;
  STDOUT->autoflush(1);
  open STDERR, ">&STDOUT";

  createBackup();

  say "Starting run at ", ts();

  if ($runType eq 'nightly') {
    fetchScores();

    fetchLeaderboards();
    fetchNeighbours();
    pruneNeighbours();

    updatePredictedAccuracy();
    updateProjectedAccuracy();
    updatePotentiallyLikedSongs();

    loadDownloadedSongs();
    fetchBeatsaverData();
    renameDownloadedSongs();

    updateSongStats();

    fetchNewSongs();

    mergeSongFiles();

    getAcoustIDs();

    identifyDuplicateSongs();
    updateSongStats();

    $dbh->do("PRAGMA incremental_vacuum");
    $dbh->do("PRAGMA optimize");
  }

  if ($runType eq 'postPlay') {
    # in case files were deleted.
    loadDownloadedSongs();

    loadFavourites();
    loadVotedSongs();

    updatePotentiallyLikedSongs();

    loadBannedSongs();
    loadDuplicateSongs();

    loadBeatSaviorData();
    pruneBeatSaviorData();

    loadYURFitData();
    pruneYURFitData();

    updatePredictedAccuracy();
    updateProjectedAccuracy();

    updateSongStats();
  }

  if ($runType eq 'prePlay') {
    # TODO compose playlist image by tiling first 4 distinct images in playlist.
    # image is 250 - 600px square, can be .jpg or .png

    writeSongsWithoutBeatSaviourStats();
    writeSongsToImprove();
    writeNotPlayedSongs();
    writeWorkout();
    writeWorkout2();

    updateSettings();
  }

  say "Run complete at ", ts();
  createBackup();
} else {
  binmode(STDOUT, ":utf8");
  binmode(STDERR, ":utf8");

  #updateProjectedAccuracy();
  mergeSongFiles();

  exit 0;
}
