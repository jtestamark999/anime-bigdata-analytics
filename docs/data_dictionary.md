##  Data Dictionary: Mega Anime Dataset

The following table defines the schema for the cleaned and  Consolidated zones of the pipeline. All data is standardized from Jikan, AniList, and Kaggle sources.

**Field Name** |   `Type`  
 Description

**mal_id**       |`Double`  
 Unique ID from MyAnimeList (Primary Key / Join Key)

**title** |`String`
Primary Japanese/Romaji title

**title_english**|`String`
Official English title

**release_year**|`Integer`
Year the anime first aired (extracted via Regex)

**score**|`Double`
Average rating (0.00 to 10.00)

**episodes**|`Double`
Total episodes produced (stored as Double for decimal compatibility)

**duration**|`String`
Length per episode (e.g., '24 min per ep')

**favorites**|`Double`
Total users who favorited the entry

**type**|`String`
Format (TV, Movie, OVA, Special, etc.)

**status**|`String`
Airing status (Finished Airing, Currently Airing, etc.)

**genres**|`Array<String>`
List of genres (Action, Sci-Fi, Award Winning, etc.)

**source**|`String`
Original material (Manga, Light Novel, Original, etc.)

**season**|`String`
Airing season (spring, summer, fall, winter)

**studios**|`Array<String>`
Production companies (Bone, Mappa, etc.)

**themes**|`Array<String>`
Narrative themes (Mecha, Space, Adult Cast, etc.)

**rank**|`Double`
Global popularity/score rank

**rating**|`String`
Age classification (e.g., R - 17+, PG-13)

