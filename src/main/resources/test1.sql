USE [Shop]
GO

SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
ALTER proc [dbo].[pre_GetSomething]    
 @Top_XID int,      
 @KoXXXID int,  
 @dod_xrk nvarchar(50)   
as       
      
declare @TempXXNo bigint, @SeXXID int , @Depo dt_Depo      
      
select @TempXXNo=TempXXNo, @SeXXID=SeXXID, @Depo=Depo      
from tb_xar_bas (nolock) where TopXXID=@Top_XID and KoXXXID=@KoXXXID       
      
if object_id('tempdb..#tmp_xxxly')>0      
 drop table #tmp_xxxly      
      
select b.TempXXNo, b.SeXXID, db.From_DXXNo, db.To_DepX, d.Ur_xxID Ur_XXID1, r.Ur_XXID2, 
d.Mik_AS_XX Aso_a_bar, r.Mik_AS_XX Reseller_bar      
into #tmp_xxxly      
from tb_xar_bas b (nolock)      
inner join tb_Dept_xxBak db (nolock) on db.From_DXXNo=b.Depo and db.SeXXID=b.SeXXID      
inner join tb_cargo_Del d (nolock) on b.TempXXNo=d.TempXXNo      
left join tb_Xun_Reduc r (nolock) on r.Ur_XXID1=d.Ur_xxID      
where b.SeXXID=@SeXXID and b.Depo=@Depo      
      
update #tmp_xxxly set Ur_XXID2 = Ur_XXID1, Reseller_bar=1 where Ur_XXID2 is null      
      
      
select sum(Aso_a_bar*Reseller_bar) Kon_cXbtar       
from #tmp_xxxly i (nolock)      
inner join tb_Urun u (nolock) on u.Ur_xxID=i.Ur_XXID2      
where TempXXNo=@TempXXNo and u.dod_xrk = @dod_xrk  
group by TempXXNo, u.dod_xrk 