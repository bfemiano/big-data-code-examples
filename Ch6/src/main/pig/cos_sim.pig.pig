plays               = load ‘/data/audioscrobbler/user_artist_data.txt’
			            using PigStorage(‘ ‘) as (user_id:long, artist_id:long, playcount:long);

artist              = load ‘/data/audioscrobbler/artist_data.txt’ as (artist_id:long, artist_name:chararray);

plays               = sample plays .01;

user_total_grp      = group plays by user_id;

user_total          = foreach user_total_grp generate group as user_id, SUM(plays.playcount) as totalplays;

plays_user_total    = join plays by user_id, user_total by user_id using ‘replicated’;

norm_plays          = foreach plays_user_total generate user_total::user_id as user_id, artist_id, ((double)playcount/(double)totalplays) * 100.0 as norm_play_cnt;

norm_plays2         = foreach norm_plays generate *;

play_pairs          = join norm_plays by user_id, norm_plays2 by user_id using ‘replicated’;

play_pairs          = filter play_pairs by norm_plays::plays::artist_id != norm_plays2::plays::artist_id;

cos_sim_step1       = foreach play_pairs generate ((double)norm_plays::norm_play_cnt) * (double)norm_plays2::norm_play_cnt) as dot_product_step1, 
                    ((double)norm_plays::norm_play_cnt *(double) norm_plays::norm_play_cnt) as play1_sq;
                    ((double)norm_plays2::norm_play_cnt *(double) norm_plays2::norm_play_cnt) as play2_sq;

cos_sim_grp         = group cos_sim_step1 by (norm_plays::plays::artist_id, norm_plays2::plays::artist_id);

cos_sim_step2       = foreach cos_sim_grp generate flatten(group), COUNT(cos_sim_step1.dot_prodct_step1) as cnt, SUM(cos_sim_step1.dot_product_step1) as dot_product, 
                        SUM(cos_sim_step1.norm_plays::norm_play_cnt) as tot_play_sq, SUM(cos_sim_step1.norm_plays2::norm_play_cnt) as tot_play_sq2;

cos_sim             = foreach cos_sim_step2 generate group::norm_plays::plays::artist_id as artist_id1, group::norm_plays2::plays_artist_id as artist_id2, 
                        dot_product / (tot_play_sq1 * tot_play_sq2) as cosine_similarity; 

art1                = join cos_sim by artist_id1, artist by artist_id using ‘replicated’;
art2                = join art1 by artist_id2, artist by artist_id using ‘replicated’;
art3                = foreach art2 generate artist_id1, art1::artist::artist_name as artist_name1, artist_id2, artist::artist_name as artist_name2, cosin_similarity;

top                 = order art3 by cosine_similarity DESC;
top_25              = limit top 25;
dump top25;

