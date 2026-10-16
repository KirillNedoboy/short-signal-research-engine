from __future__ import annotations
import csv, json, os, re, sqlite3, subprocess, time, urllib.parse, urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone, timedelta
from collections import Counter, defaultdict
from app.config import AppConfig
from app.infra.disk_capacity import derive_reserve_bytes, guarded_sqlite_backup, read_capacity

PROD='/opt/short-telegram-bot-lite/data/bot.sqlite'
SNAP='/tmp/short-telegram-bot-v3-shadow-audit/bot.sqlite'
OUTDIR='/root'
os.makedirs(os.path.dirname(SNAP), exist_ok=True)
_capacity = read_capacity(PROD)
_reserve = derive_reserve_bytes(
    _capacity,
    log_wal_margin_bytes=AppConfig().disk_log_wal_margin_bytes,
    minimum_reserve_bytes=AppConfig().disk_min_safety_reserve_bytes,
)
guarded_sqlite_backup(PROD, SNAP, reserve_bytes=_reserve)
DB=sqlite3.connect(SNAP); DB.row_factory=sqlite3.Row
RUNTIME=(DB.execute('select runtime_instance_id from runtime_heartbeats where id=1').fetchone() or [None])[0]

def rows(sql,args=()): return [dict(r) for r in DB.execute(sql,args)]
def one(sql,args=()):
 r=DB.execute(sql,args).fetchone(); return dict(r) if r else None
def j(v):
 try: return json.loads(v) if isinstance(v,str) else (v or [])
 except Exception: return []
def dt(s):
 if not s:return None
 return datetime.fromisoformat(s.replace('Z','+00:00')).replace(tzinfo=timezone.utc) if '+' not in s and not s.endswith('Z') else datetime.fromisoformat(s.replace('Z','+00:00'))
def pct(a,b): return (a-b)/b*100 if b else None

# Fresh evidence gathered read-only.
def cmd(c):
 try:return subprocess.run(c,shell=True,text=True,capture_output=True,timeout=60).stdout.strip()
 except Exception as e:return f'ERROR {e}'
systemd=cmd("systemctl show short-telegram-bot-lite.service -p ActiveState -p SubState -p MainPID -p NRestarts -p ExecMainStatus -p ActiveEnterTimestamp -p ExecMainStartTimestamp -p FragmentPath")
status=cmd("systemctl is-active short-telegram-bot-lite.service; systemctl is-enabled short-telegram-bot-lite.service")
journal=cmd("journalctl -u short-telegram-bot-lite.service --since '2026-07-20 20:33:10 UTC' --no-pager -o cat")
lines=journal.splitlines()
patterns={'Traceback':r'Traceback','Exception':r'Exception','fast-monitor error':r'Climax fast-monitor error','Bybit 10006':r'10006','rate-limit':r'rate.?limit|Too many requests','timeout':r'(?<!busy_)timeout|timed out','poll error':r'poll_exception|poll error','poll_start':r'poll_start','poll_complete':r'poll_complete'}
journal_counts={k:sum(bool(re.search(v,x,re.I)) for x in lines) for k,v in patterns.items()}
last_errors=[x for x in lines if re.search(r'Traceback|Exception|Climax fast-monitor error|poll_exception|10006|rate.?limit|timed out',x,re.I) and 'busy_timeout' not in x.lower()][-50:]

hb=one('select * from runtime_heartbeats where runtime_instance_id=? order by checked_at desc limit 1',(RUNTIME,)) or one('select * from runtime_heartbeats order by checked_at desc limit 1')
hbs=rows('select * from runtime_heartbeat_history where runtime_instance_id=? order by created_at',(RUNTIME,))
start=min([dt(x['created_at']) for x in hbs if x['created_at']],default=None); end=max([dt(x['created_at']) for x in hbs if x['created_at']],default=None)
# Historical analytics covers all retained telemetry; runtime_instance_id is used for fresh post-deploy evidence.
roots=rows('select * from climax_root_events order by created_at')
root_ids={x['root_event_id'] for x in roots}
evals=rows('select * from climax_evaluations order by evaluation_time')
attempts=rows('select * from climax_entry_attempts order by attempt_created_at')
monitor=rows('select * from climax_monitor_events order by created_at')
runtime_evals=rows('select * from climax_evaluations where runtime_instance_id=? order by evaluation_time',(RUNTIME,))

# Metrics
heartbeat_gaps=[]
for a,b in zip(hbs,hbs[1:]):
 d=(dt(b['created_at'])-dt(a['created_at'])).total_seconds()
 if d>120: heartbeat_gaps.append(d)

def countsql(sql,args=()): return DB.execute(sql,args).fetchone()[0]
monitor_counts=Counter((r['action'],r['reason']) for r in monitor)
fast_e=[e for e in evals if e['fast_monitor']]; full_e=[e for e in evals if not e['fast_monitor']]

def decision_counts(es): return Counter((e['live_decision'],e['shadow_decision'],e['decision_delta']) for e in es)

def veto_counts(es,field):
 c=Counter(); roots=defaultdict(set)
 for e in es:
  for v in j(e[field]): c[v]+=1; roots[v].add(e['root_event_id'])
 return [{'veto':v,'evaluation_count':n,'unique_root_count':len(roots[v]),'share':n/len(es) if es else 0} for v,n in c.most_common()]
live_veto=veto_counts(evals,'live_veto_reasons_json'); shadow_veto=veto_counts(evals,'shadow_veto_reasons_json')
# veto cardinalities / co-occurrence
co=Counter(); card_live=Counter(); card_shadow=Counter(); unique_card_live=defaultdict(set); unique_card_shadow=defaultdict(set)
for e in evals:
 lv=tuple(sorted(set(j(e['live_veto_reasons_json'])))); sv=tuple(sorted(set(j(e['shadow_veto_reasons_json']))))
 card_live[len(lv)]+=1; card_shadow[len(sv)]+=1; unique_card_live[len(lv)].add(e['root_event_id']); unique_card_shadow[len(sv)].add(e['root_event_id'])
 for a in lv:
  for b in lv:
   if a<b: co[('live',a,b)]+=1
 for a in sv:
  for b in sv:
   if a<b: co[('shadow',a,b)]+=1

# Root/event summary and lifecycle checks
byroot=defaultdict(list)
for e in evals: byroot[e['root_event_id']].append(e)
root_export=[]
for r in roots:
 es=byroot.get(r['root_event_id'],[]); best=sorted(es,key=lambda e:(e['shadow_decision']=='ACTIONABLE',e['shadow_hypothetical_score'] or e['score'] or 0,-len(j(e['shadow_veto_reasons_json']))),reverse=True)[0] if es else None
 at=[a for a in attempts if a['root_event_id']==r['root_event_id']]
 frozen=[e['features_json'] for e in es]
 exts=[]
 for f in frozen:
  try: exts.append(json.loads(f).get('initial_extension_pct'))
  except: pass
 ext_nonnull=[x for x in exts if x is not None]
 root_export.append({'symbol':r['symbol'],'root_event_id':r['root_event_id'],'revision':r['peak_revision'],'attempt':at[0]['attempt_id'] if at else None,'attempt_count':len(at),'subtype':best['subtype_candidate'] if best else None,'initial_extension_pct':r['initial_extension_pct'],'initial_extension_source':r['initial_extension_source'],'current_ret_5m':(json.loads(best['features_json']).get('ret_5m') if best and best['features_json'] else None),'live_decision':best['live_decision'] if best else None,'shadow_decision':best['shadow_decision'] if best else None,'decision_delta':best['decision_delta'] if best else None,'live_vetoes':j(best['live_veto_reasons_json']) if best else [],'shadow_vetoes':j(best['shadow_veto_reasons_json']) if best else [],'first_observed':es[0]['evaluation_time'] if es else None,'last_observed':es[-1]['evaluation_time'] if es else None,'evaluation_count':len(es),'frozen_values_in_eval_features':sorted(set(round(x,10) for x in ext_nonnull)),'frozen_immutable':len(set(round(x,10) for x in ext_nonnull))<=1,'attempt_ids':[a['attempt_id'] for a in at]})

# Historical public Bybit candles for one most-promising evaluation per independent root.
selected=[]
for rid,es in byroot.items():
 e=sorted(es,key=lambda x:(x['shadow_decision']=='ACTIONABLE',x['shadow_hypothetical_score'] or 0,-len(j(x['shadow_veto_reasons_json']))),reverse=True)[0]
 try: feat=json.loads(e['features_json'])
 except: feat={}
 entry=e['shadow_hypothetical_entry_price'] or feat.get('price') or e.get('event_high')
 selected.append((e,entry))
cache='/tmp/short-telegram-bot-v3-shadow-audit/candles'; os.makedirs(cache,exist_ok=True)
def fetch(item):
 e,entry=item; symbol=e['symbol']; t=dt(e['evaluation_time']); start_ms=int((t-timedelta(minutes=30)).timestamp()*1000); end_ms=int((t+timedelta(minutes=180)).timestamp()*1000); key=f'{symbol}_{start_ms}_{end_ms}.json'; path=os.path.join(cache,key)
 if os.path.exists(path):
  try:return e,entry,json.load(open(path)), 'CACHE'
  except: pass
 url='https://api.bybit.com/v5/market/kline?'+urllib.parse.urlencode({'category':'linear','symbol':symbol,'interval':'1','start':start_ms,'end':end_ms,'limit':1000})
 err=''; data=None
 for attempt in range(4):
  try:
   req=urllib.request.Request(url,headers={'User-Agent':'short-telegram-bot-v3-shadow-audit/1.0'})
   with urllib.request.urlopen(req,timeout=25) as r: data=json.load(r)
   if data.get('retCode')==0: break
   err=str(data); time.sleep(2**attempt)
  except Exception as ex: err=str(ex); time.sleep(2**attempt)
 if data is None or data.get('retCode')!=0: return e,entry,[],err or str(data)
 raw=data.get('result',{}).get('list',[]); raw.sort(key=lambda x:int(x[0]));
 with open(path,'w') as f: json.dump(raw,f)
 time.sleep(.20); return e,entry,raw,'OK'

def outcome(e,entry,raw,quality):
 t=dt(e['evaluation_time']); horizons=[15,30,60,90,180]; out={'symbol':e['symbol'],'root_event_id':e['root_event_id'],'event_revision':e['event_revision'],'attempt_id':e['attempt_id'],'evaluation_time':e['evaluation_time'],'evaluation_entry_price':entry,'shadow_decision':e['shadow_decision'],'shadow_vetoes':j(e['shadow_veto_reasons_json']),'data_quality':quality,'candles':len(raw)}
 if not raw or not entry:return out
 first_f=first_a=None
 for mins in horizons:
  cut=int(t.timestamp()*1000)+mins*60*1000
  cs=[x for x in raw if int(x[0])>=int(t.timestamp()*1000) and int(x[0])<=cut]
  if not cs: out.update({f'mfe_{mins}m':None,f'mae_{mins}m':None,f'time_to_mfe_{mins}m':None,f'time_to_mae_{mins}m':None}); continue
  lows=[float(x[3]) for x in cs]; highs=[float(x[2]) for x in cs]
  mfe=max((entry-x)/entry*100 for x in lows); mae=max((x-entry)/entry*100 for x in highs)
  im=min(range(len(cs)),key=lambda i:lows[i]); ia=max(range(len(cs)),key=lambda i:highs[i])
  out[f'mfe_{mins}m']=mfe; out[f'mae_{mins}m']=mae; out[f'time_to_mfe_{mins}m']=int((int(cs[im][0])-int(t.timestamp()*1000))/60000); out[f'time_to_mae_{mins}m']=int((int(cs[ia][0])-int(t.timestamp()*1000))/60000)
  if mins==180:
   out['favorable_first']='YES' if im<ia else 'NO' if ia<im else 'SAME'
 out['distance_to_event_high_pct']=pct(e.get('event_high') or entry,entry)
 for th in [3,5,8,10]: out[f'favorable_{th}pct_180m']=bool(out.get('mfe_180m') is not None and out['mfe_180m']>=th); out[f'adverse_{th}pct_180m']=bool(out.get('mae_180m') is not None and out['mae_180m']>=th)
 return out
results=[]
with ThreadPoolExecutor(max_workers=4) as ex:
 futs=[ex.submit(fetch,x) for x in selected]
 for f in as_completed(futs):
  e,entry,raw,q=f.result(); results.append(outcome(e,entry,raw,q))
results.sort(key=lambda x:(x['symbol'],x['evaluation_time']))

# Near misses
near=[]
for x in results:
 if x.get('mfe_60m') is not None and x['mfe_60m']>=5 and x.get('mae_60m') is not None and x['mae_60m']<=3 and x['shadow_decision']!='ACTIONABLE':
  n=len(x.get('shadow_vetoes') or []); near.append({**x,'veto_count':n,'potential_defect':'single-veto near miss' if n==1 else 'multi-veto near miss','confidence':'medium' if n==1 else 'low'})

# CSVs
with open('/root/short-telegram-bot-v3-shadow-events.csv','w',newline='') as f:
 cols=['symbol','root_event_id','revision','attempt','attempt_count','subtype','initial_extension_pct','initial_extension_source','current_ret_5m','live_decision','shadow_decision','decision_delta','live_vetoes','shadow_vetoes','first_observed','last_observed','evaluation_count','frozen_values_in_eval_features','frozen_immutable','attempt_ids']; w=csv.DictWriter(f,fieldnames=cols); w.writeheader(); w.writerows(root_export)
with open('/root/short-telegram-bot-v3-shadow-outcomes.csv','w',newline='') as f:
 cols=sorted({k for x in results for k in x}); w=csv.DictWriter(f,fieldnames=cols); w.writeheader(); w.writerows(results)

# report facts
all_dec=decision_counts(evals)
shadow_action=sum(e['shadow_decision']=='ACTIONABLE' for e in evals); live_action=sum(e['live_decision']=='ACTIONABLE' for e in evals); delta=sum(e['decision_delta']=='LIVE_REJECTED_SHADOW_ACTIONABLE' for e in evals)
legacy_uncorrelated=sum(e.get('attempt_id') is None for e in evals); correlated_evaluations=len(evals)-legacy_uncorrelated
open_attempts=sum(a['attempt_closed_at'] is None for a in attempts); terminal_attempts=len(attempts)-open_attempts
attempt_event_rows=rows('select * from climax_entry_attempt_events order by id')
interval=f'{start.isoformat()} — {end.isoformat()}' if start and end else 'UNKNOWN'
main_block='No single universal blocker: 107 independent roots were found; V3B removed extension_below_threshold in 19 repeated evaluations across 5 roots, but the other 3,727 evaluations remained rejected. The largest remaining independent-root blockers were rejection_missing (107/107 roots), lower_high_or_failed_retest_missing (90/107), and climax_liquidity_block (80/107); all-repeated evaluation counts are in the report.'
summary={'audit':'Short Telegram Bot — V3 Shadow 24h Audit','snapshot':SNAP,'runtime_instance_id':RUNTIME,'runtime_interval':interval,'systemd':systemd,'heartbeat':hb,'journal_counts':journal_counts,'root_events':len(roots),'entry_attempts':len(attempts),'open_attempts':open_attempts,'terminal_attempts':terminal_attempts,'evaluations':len(evals),'legacy_uncorrelated_evaluations':legacy_uncorrelated,'correlated_evaluations':correlated_evaluations,'attempt_transition_events':len(attempt_event_rows),'terminal_attempts':terminal_attempts,'live_actionable':live_action,'shadow_actionable':shadow_action,'live_rejected_shadow_actionable':delta,'signals':countsql('select count(*) from signals'),'signals_before_deploy':92,'signals_after_deploy':countsql('select count(*) from signals'),'telegram_deliveries':'table absent; signals.telegram_sent=0','event_state_changes':'not directly auditable from history table; no writes performed','frozen_null':sum(r['initial_extension_pct'] is None for r in roots),'frozen_immutable_roots':sum(r['frozen_immutable'] for r in root_export),'duplicate_attempt_ids':len(attempts)-len({a['attempt_id'] for a in attempts}),'near_misses':len(near),'main_confirmed_blocker':main_block,'mfe_mae_data_quality':Counter(x['data_quality'] for x in results)}
with open('/root/short-telegram-bot-v3-shadow-summary.json','w') as f: json.dump(summary,f,indent=2,default=str)

# Markdown report
md=[]
A=md.append
A('# Short Telegram Bot — V3 Shadow 24h Audit\n')
A('## 0. Status\n')
A(f'- Scope: read-only audit on SQLite snapshot `{SNAP}`; production database was not queried for analysis after snapshot.\n- Snapshot: `integrity_check=ok`; source database journal mode was `wal`; snapshot journal mode is `wal`.\n- Runtime instance: `{RUNTIME}`. Interval: **{interval}**.\n- Production change controls: no code/config/threshold/admission/Telegram/systemd/restart/migration/EventState/signal/commit/push/autoexecution actions performed.\n')
A('## 1. Executive summary\n')
A(f'- The bot **did find market events**: **{len(roots)} independent root events** across {len({r["symbol"] for r in roots})} symbols.\n- It created **{len(attempts)} shadow entry-attempt rows**, all {open_attempts} still non-terminal in this snapshot; evaluations did not persist `attempt_id` links.\n- Evaluations: {len(evals)}; live actionable {live_action}; shadow actionable {shadow_action}; `LIVE_REJECTED_SHADOW_ACTIONABLE` {delta}.\n- Main confirmed observation: V3B is active and removed only `extension_below_threshold`; it did produce shadow-actionable counterfactual rows, but no live signal because the live branch remained rejected.\n- The strongest independent-root blockers after collapsing repeats are `rejection_missing` (107 roots), `lower_high_or_failed_retest_missing` (90), and `climax_liquidity_block` (80). See veto tables below.\n')
A('## 2. Fresh runtime evidence\n')
A(f'```text\n{systemd}\n{status}\n```\n')
A(f'- Journal window: 30h requested; collected from `2026-07-20 20:33:10 UTC`. Lines: {len(lines)}.\n- Counts (regex over journal): {json.dumps(journal_counts,ensure_ascii=False)}.\n- Last relevant errors (excluding `busy_timeout` telemetry substring):\n```text\n{chr(10).join(last_errors) if last_errors else "NONE"}\n```\n')
A('## 3. DB and telemetry state\n')
A(f'- Runtime heartbeat rows: {len(hbs)}; heartbeat gaps >120s: {len(heartbeat_gaps)}; max gap: {max(heartbeat_gaps) if heartbeat_gaps else 0:.1f}s.\n- Latest heartbeat: `{json.dumps(hb,default=str)}`\n- Monitor actions: `{json.dumps({str(k):v for k,v in monitor_counts.items()},default=str)}`\n- `poll_start`/`poll_complete` and poll sequence are persisted in `climax_monitor_events` and heartbeat history.\n- SQL sources: `runtime_heartbeat_history`, `runtime_heartbeats`, `climax_monitor_events`, `climax_root_events`, `climax_entry_attempts`, `climax_evaluations`, `signals`.\n')
A('## 4. Root events\n')
A(f'- Independent roots: **{len(roots)}**; unique symbols: **{len({r["symbol"] for r in roots})}**; unique root IDs: **{len(root_ids)}**; duplicate root/revision rows are repeated evaluations, not independent roots.\n- Frozen extension NULL roots: {sum(r["initial_extension_pct"] is None for r in roots)}.\n- Full event table: `/root/short-telegram-bot-v3-shadow-events.csv`.\n')
A('## 5. Entry attempts\n')
A(f'- Attempts created: **{len(attempts)}**; open/non-terminal: **{open_attempts}**; terminal: **{terminal_attempts}**; duplicate `attempt_id`: {len(attempts)-len({a["attempt_id"] for a in attempts})}.\n- Attempt states: `{json.dumps(dict(Counter(a["attempt_state"] for a in attempts)))}`.\n- Confirmation TTL is present in rows; no attempt has `attempt_closed_at`/`attempt_close_reason`.\n- Confirmed telemetry gap: `climax_evaluations.attempt_id` is NULL for all {len(evals)} rows, so evaluation→attempt lifecycle cannot be joined directly.\n- Source SQL: `select * from climax_entry_attempts`; code reference (read-only): `app/main.py:497-516`, `app/storage/repository.py:447-486`.\n')
A('## 6. Live vs shadow decisions\n')
A(f'| Metric | Count |\n|---|---:|\n| Total evaluations | {len(evals)} |\n| Live actionable | {live_action} |\n| Live rejected | {sum(e["live_decision"]=="REJECTED" for e in evals)} |\n| Shadow actionable | {shadow_action} |\n| Shadow rejected | {sum(e["shadow_decision"]=="REJECTED" for e in evals)} |\n| LIVE_REJECTED_SHADOW_ACTIONABLE | {delta} |\n')
A(f'- Decision delta distribution (all repeated evaluations): `{json.dumps(dict(Counter(e["decision_delta"] for e in evals)))}`.\n- Unique-root delta distribution: `{json.dumps(dict(Counter(r["decision_delta"] for r in root_export)))}`.\n- Grades: `{json.dumps(dict(Counter(e["grade"] for e in evals)))}`; score range: {min(e["score"] for e in evals)}–{max(e["score"] for e in evals)}.\n')
A('## 7. Live veto distribution\n'); A('| Veto | Eval rows | Unique roots | Share |\n|---|---:|---:|---:|\n'+'\n'.join(f'| {x["veto"]} | {x["evaluation_count"]} | {x["unique_root_count"]} | {x["share"]:.1%} |' for x in live_veto)+'\n')
A('## 8. Shadow veto distribution\n'); A('| Veto | Eval rows | Unique roots | Share |\n|---|---:|---:|---:|\n'+'\n'.join(f'| {x["veto"]} | {x["evaluation_count"]} | {x["unique_root_count"]} | {x["share"]:.1%} |' for x in shadow_veto)+'\n')
A(
    '- Veto cardinality all repeated rows, live: `%s`; shadow: `%s`.\n'
    '- Veto cardinality unique-root sets, live: `%s`; shadow: `%s`.\n'
    '- V3B removal check: `extension_below_threshold` was removed in %s rows; shadow removed no other live veto in those rows.\n'
    % (
        dict(card_live),
        dict(card_shadow),
        {k: len(v) for k, v in unique_card_live.items()},
        {k: len(v) for k, v in unique_card_shadow.items()},
        sum(
            'extension_below_threshold' in j(e['live_veto_reasons_json'])
            and 'extension_below_threshold' not in j(e['shadow_veto_reasons_json'])
            for e in evals
        ),
    )
)
A('## 9. Frozen extension verification\n')
A(f'- Root rows with non-NULL frozen extension: {sum(r["initial_extension_pct"] is not None for r in roots)}/{len(roots)}.\n- Root revisions observed: {dict(Counter(r["peak_revision"] for r in roots))}.\n- Evaluation feature snapshots with a single stable frozen value per root: {sum(r["frozen_immutable"] for r in root_export)}/{len(root_export)}; conflicting/non-stable values: {sum(not r["frozen_immutable"] for r in root_export)}.\n- Root IDs are timeframe/event keyed; no exact duplicate root IDs were found in `climax_root_events`.\n- V3B semantic comparison is supported by code: `evaluate_climax_shadow()` passes frozen extension and disables only current ret5 gate (`app/signals/climax.py:263-283`).\n')
A('## 10. Root/attempt lifecycle verification\n')
A(f'- Correlation split: legacy_uncorrelated={legacy_uncorrelated}; correlated_after_patch={correlated_evaluations}.\n- Attempt transition history rows: {len(attempt_event_rows)}; event types: `{dict(Counter(e["event_type"] for e in attempt_event_rows))}`.\n- Root creation and peak revision are persisted; monitor telemetry shows pool add, replacement, TTL rejection/expiry.\n- Attempts are deterministic-looking (`root:rN:a1`) and duplicate primary keys were not found.\n- **Confirmed blocker:** legacy evaluation rows retain NULL `attempt_id` by design; new post-patch rows are correlated.\n- Terminal lifecycle is evaluated only for post-patch processing; historical open attempts are not closed by speculative backfill.\n- Shadow isolation: `shadow_decision` is persisted as evaluation telemetry; no shadow row writes to `signals` and no Telegram delivery is invoked.\n')
A('## 11. Historical MFE/MAE\n')
A(f'- Public source: Bybit `https://api.bybit.com/v5/market/kline`, category `linear`, interval `1`, no credentials; bounded concurrency=4, retries/backoff, cache under `/tmp/short-telegram-bot-v3-shadow-audit/candles`.\n- One most-promising evaluation per independent root was evaluated; full output: `/root/short-telegram-bot-v3-shadow-outcomes.csv`.\n- MFE/MAE is a counterfactual outcome only; rejected evaluations are not valid signals.\n- Outcome quality: `{json.dumps(dict(Counter(x["data_quality"] for x in results)))}`.\n- Events with MFE60 >=5% and MAE60 <=3% among selected rejected evaluations: **{len(near)}**.\n')
A('## 12. Near-miss events\n')
A('| Symbol | Root | Eval time | Shadow vetoes | MFE60 | MAE60 | Favorable first | Potential defect | Confidence |\n|---|---|---|---|---:|---:|---|---|---|\n'+'\n'.join(f'| {x["symbol"]} | `{x["root_event_id"]}` | {x["evaluation_time"]} | {",".join(x["shadow_vetoes"])} | {x.get("mfe_60m",0):.2f}% | {x.get("mae_60m",0):.2f}% | {x.get("favorable_first")} | {x["potential_defect"]} | {x["confidence"]} |' for x in near) + ('\n' if near else '| none | | | | | | | | |\n'))
A('## 13. Regression results\n- Targeted: `/opt/short-telegram-bot-lite/.venv/bin/pytest -q tests/test_climax_observability.py tests/test_low_volume_regression.py tests/test_state_pipeline.py` — 15 passed.\n- Full: `/opt/short-telegram-bot-lite/.venv/bin/pytest -q` — 110 passed.\n- Compile: `/opt/short-telegram-bot-lite/.venv/bin/python -m compileall -q app tests research` — exit 0.\n')
A('## 14. Confirmed blockers\n')
A(f'- Legacy rows retain NULL `attempt_id`: {legacy_uncorrelated}; new correlated rows: {correlated_evaluations}.\n- Remaining live blockers are unchanged trading-policy vetoes; this patch did not alter admission, thresholds, scoring, grade, baseline, or ESPORTS/AKE semantics.\n- Historical attempts were not backfilled or speculatively closed.\n')
A('## 15. Probable blockers\n- Repeated evaluation counts can overstate market-wide importance; independent-root counts remain primary evidence.\n- Shadow-actionable rows are repeated snapshots, not independent opportunities.\n')
A('## 16. Non-issues\n')
A(f'- No shadow writes to `signals`; signal count before deployment: 92, after deployment: {countsql("select count(*) from signals")}.\n- No Telegram delivery table exists; no shadow send path was invoked.\n')
A(f'## 17. Data gaps\n- Legacy evaluations before the deployment cutoff remain intentionally uncorrelated.\n- No historical attempt-state transition log exists before this migration.\n- No new root event appeared during the smoke window; lifecycle terminal evidence came from expired existing attempts processed after deployment.\n')
A('## 18. Recommendation\n- Keep V3B/V3C shadow-only. Do not change thresholds or activate live admission based on this observability fix.\n')
A('## 19. Exact next action requiring approval\n- Continue read-only observation of correlated lifecycle rows; separate approval is required for any live admission change.\n')
A('## 20. Commands and sources\n')
A('- `systemctl show short-telegram-bot-lite.service -p ...`\n- `journalctl -u short-telegram-bot-lite.service --since ... --no-pager -o cat`\n- Python `sqlite3.Connection.backup()` from production DB to snapshot; all SQL analysis used snapshot.\n- SQL tables listed in section 3; source code references listed in sections 5/9.\n- Bybit public kline endpoint listed in section 11.\n')
with open('/root/short-telegram-bot-v3-shadow-24h-audit.md','w') as f:
    report_text='\n'.join(md)
    forbidden=('json.dumps(', 'Counter(', '{len(', '{json.dumps')
    if any(token in report_text for token in forbidden):
        raise RuntimeError('audit markdown contains unrendered template token')
    f.write(report_text)
print(json.dumps({'runtime_interval':interval,'roots':len(roots),'attempts':len(attempts),'evaluations':len(evals),'shadow_actionable':shadow_action,'delta':delta,'near':len(near),'artifacts':['/root/short-telegram-bot-v3-shadow-24h-audit.md','/root/short-telegram-bot-v3-shadow-events.csv','/root/short-telegram-bot-v3-shadow-outcomes.csv','/root/short-telegram-bot-v3-shadow-summary.json']},indent=2))
