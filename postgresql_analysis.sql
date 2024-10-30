SELECT * FROM public.facebookshares f
inner join public.articles a
on f.article_id = a.id
where facebook_share is not null
order by facebook_share desc

