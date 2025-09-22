import pandas as pd
import asyncio
import aiohttp
from datetime import datetime, date
from app.database import AsyncSessionLocal
from app.models import (
    Processo,
    ProcessoRelacionado,
    Fonte,
    Capa,
    ValorCausa,
    InformacaoComplementar,
    Envolvido,
    Advogado,
    OAB,
    Audiencia,
)
from app.consultas import consultar_numero
from sqlalchemy.future import select

BATCH_SIZE = 200

async def salvar_processo(session, data):
    """Salva o processo e seus relacionamentos no banco."""
    q = await session.execute(select(Processo).where(Processo.numero_cnj == data["numero_cnj"]))
    processo = q.scalar_one_or_none()

    if not processo:
        # CONVERSÕES DE DATAS E TIMESTAMPS PARA O MODELO PROCESSO
        data_inicio = data.get("data_inicio")
        if data_inicio and isinstance(data_inicio, str):
            try:
                data_inicio = datetime.strptime(data_inicio, "%Y-%m-%d").date()
            except ValueError:
                data_inicio = None
        
        data_ultima_movimentacao = data.get("data_ultima_movimentacao")
        if data_ultima_movimentacao and isinstance(data_ultima_movimentacao, str):
            try:
                data_ultima_movimentacao = datetime.strptime(data_ultima_movimentacao, "%Y-%m-%d").date()
            except ValueError:
                data_ultima_movimentacao = None

        data_ultima_verificacao = data.get("data_ultima_verificacao")
        if data_ultima_verificacao and isinstance(data_ultima_verificacao, str):
            try:
                data_ultima_verificacao = datetime.fromisoformat(data_ultima_verificacao)
            except ValueError:
                data_ultima_verificacao = None
        
        processo = Processo(
    numero_cnj=data.get("numero_cnj"),
    titulo_polo_ativo=data.get("titulo_polo_ativo"),
    titulo_polo_passivo=data.get("titulo_polo_passivo"),
    ano_inicio=data.get("ano_inicio"),
    data_inicio=data_inicio,
    estado_origem=(
        data.get("estado_origem", {}).get("sigla")
        if isinstance(data.get("estado_origem"), dict)
        else data.get("estado_origem")
    ),
    data_ultima_movimentacao=data_ultima_movimentacao,
    quantidade_movimentacoes=data.get("quantidade_movimentacoes"),
    fontes_tribunais_estao_arquivadas=data.get("fontes_tribunais_estao_arquivadas"),
    tempo_desde_ultima_verificacao=data.get("tempo_desde_ultima_verificacao"),
    data_ultima_verificacao=data_ultima_verificacao,

    unidade_origem_nome=data.get("unidade_origem", {}).get("nome"),
    unidade_origem_cidade=data.get("unidade_origem", {}).get("cidade"),
    unidade_origem_estado=data.get("unidade_origem", {}).get("estado"),
    unidade_origem_tribunal_sigla=data.get("unidade_origem", {}).get("tribunal_sigla"),
)

        session.add(processo)
        await session.flush()

        # Processos relacionados
        for rel in data.get("processos_relacionados") or []:
            session.add(ProcessoRelacionado(
                processo_id=processo.id,
                numero=rel.get("numero")
            ))

        # Fontes
        for f in data.get("fontes") or []:
            fonte_data_inicio = f.get("data_inicio")
            if fonte_data_inicio and isinstance(fonte_data_inicio, str):
                try:
                    fonte_data_inicio = datetime.strptime(fonte_data_inicio, "%Y-%m-%d").date()
                except ValueError:
                    fonte_data_inicio = None

            fonte_data_ultima_movimentacao = f.get("data_ultima_movimentacao")
            if fonte_data_ultima_movimentacao and isinstance(fonte_data_ultima_movimentacao, str):
                try:
                    fonte_data_ultima_movimentacao = datetime.strptime(fonte_data_ultima_movimentacao, "%Y-%m-%d").date()
                except ValueError:
                    fonte_data_ultima_movimentacao = None
            
            fonte_data_ultima_verificacao = f.get("data_ultima_verificacao")
            if fonte_data_ultima_verificacao and isinstance(fonte_data_ultima_verificacao, str):
                try:
                    fonte_data_ultima_verificacao = datetime.fromisoformat(fonte_data_ultima_verificacao)
                except ValueError:
                    fonte_data_ultima_verificacao = None
            
            fonte = Fonte(
                processo_id=processo.id,
                fonte_id=f.get("id"),
                processo_fonte_id=f.get("processo_fonte_id"),
                descricao=f.get("descricao"),
                nome=f.get("nome"),
                sigla=f.get("sigla"),
                tipo=f.get("tipo"),
                data_inicio=fonte_data_inicio,
                data_ultima_movimentacao=fonte_data_ultima_movimentacao,
                segredo_justica=f.get("segredo_justica"),
                arquivado=f.get("arquivado"),
                status_predito=f.get("status_predito"),
                grau=f.get("grau"),
                grau_formatado=f.get("grau_formatado"),
                fisico=f.get("fisico"),
                sistema=f.get("sistema"),
                url=f.get("url"),
                quantidade_envolvidos=f.get("quantidade_envolvidos"),
                data_ultima_verificacao=fonte_data_ultima_verificacao,
                quantidade_movimentacoes=f.get("quantidade_movimentacoes"),
                outros_numeros=f.get("outros_numeros"),
            )
            session.add(fonte)
            await session.flush()

            # Capa
            if f.get("capa"):
                capa_data = f["capa"]
                
                data_distribuicao = capa_data.get("data_distribuicao")
                if data_distribuicao and isinstance(data_distribuicao, str):
                    try:
                        data_distribuicao = datetime.strptime(data_distribuicao, "%Y-%m-%d").date()
                    except ValueError:
                        data_distribuicao = None
                
                data_arquivamento = capa_data.get("data_arquivamento")
                if data_arquivamento and isinstance(data_arquivamento, str):
                    try:
                        data_arquivamento = datetime.strptime(data_arquivamento, "%Y-%m-%d").date()
                    except ValueError:
                        data_arquivamento = None

                capa = Capa(
                    fonte_id=fonte.id,
                    classe=capa_data.get("classe"),
                    assunto=capa_data.get("assunto"),
                    assuntos_normalizados=capa_data.get("assuntos_normalizados"),
                    assunto_principal_normalizado=capa_data.get("assunto_principal_normalizado"),
                    area=capa_data.get("area"),
                    orgao_julgador=capa_data.get("orgao_julgador"),
                    situacao=capa_data.get("situacao"),
                    data_distribuicao=data_distribuicao,
                    data_arquivamento=data_arquivamento,
                )
                session.add(capa)
                await session.flush()

                # Valor da Causa
                if capa_data.get("valor_causa"):
                    valor_causa_data = capa_data["valor_causa"]
                    
                    valor_float = None
                    valor_str = valor_causa_data.get("valor")
                    if valor_str:
                        try:
                            # A API retorna um valor decimal como string,
                            # por isso a conversão para float é necessária.
                            valor_float = float(valor_str)
                        except (ValueError, TypeError):
                            # Em caso de valor inválido ou nulo,
                            # o valor será salvo como None no banco.
                            valor_float = None

                    session.add(ValorCausa(
                        capa_id=capa.id,
                        valor=valor_float,
                        moeda=valor_causa_data.get("moeda"),
                        valor_formatado=valor_causa_data.get("valor_formatado"),
                    ))

                # Informações Complementares
                for info in capa_data.get("informacoes_complementares") or []:
                    session.add(InformacaoComplementar(
                        capa_id=capa.id,
                        tipo=info.get("tipo"),
                        valor=info.get("valor"),
                    ))

            # Audiências
            for aud in f.get("audiencias") or []:
                data_audiencia = aud.get("data_audiencia")
                if data_audiencia and isinstance(data_audiencia, str):
                    try:
                        data_audiencia = datetime.fromisoformat(data_audiencia)
                    except ValueError:
                        data_audiencia = None
                
                session.add(Audiencia(
                    fonte_id=fonte.id,
                    data_audiencia=data_audiencia,
                    descricao=aud.get("descricao")
                ))

            # Envolvidos
            for envolvido_data in f.get("envolvidos") or []:
                env = Envolvido(
                    fonte_id=fonte.id,
                    nome=envolvido_data.get("nome"),
                    quantidade_processos=envolvido_data.get("quantidade_processos"),
                    tipo_pessoa=envolvido_data.get("tipo_pessoa"),
                    tipo=envolvido_data.get("tipo"),
                    tipo_normalizado=envolvido_data.get("tipo_normalizado"),
                    polo=envolvido_data.get("polo"),
                    cpf=envolvido_data.get("cpf"),
                    cnpj=envolvido_data.get("cnpj"),
                    prefixo=envolvido_data.get("prefixo"),
                    sufixo=envolvido_data.get("sufixo"),
                )
                session.add(env)
                await session.flush()

                # Advogados
                for adv_data in envolvido_data.get("advogados") or []:
                    adv = Advogado(
                        envolvido_id=env.id,
                        nome=adv_data.get("nome"),
                        quantidade_processos=adv_data.get("quantidade_processos"),
                        tipo=adv_data.get("tipo"),
                        tipo_normalizado=adv_data.get("tipo_normalizado"),
                        polo=adv_data.get("polo"),
                        cpf=adv_data.get("cpf"),
                        cnpj=adv_data.get("cnpj"),
                        prefixo=adv_data.get("prefixo"),
                        sufixo=adv_data.get("sufixo"),
                    )
                    session.add(adv)
                    await session.flush()

                    # OABs do advogado
                    for oab_data in adv_data.get("oabs") or []:
                        session.add(OAB(
                            advogado_id=adv.id,
                            uf=oab_data.get("uf"),
                            tipo=oab_data.get("tipo"),
                            numero=oab_data.get("numero")
                        ))
    
    await session.commit()


async def processar_lote(numeros_cnj: list):
    """Processa uma lista de CNJs em batches de BATCH_SIZE."""
    total = len(numeros_cnj)
    print(f"Total de CNJs a processar: {total}")

    for i in range(0, total, BATCH_SIZE):
        batch = numeros_cnj[i:i + BATCH_SIZE]
        print(f"Processando batch {i//BATCH_SIZE + 1} ({len(batch)} CNJs)")

        async with aiohttp.ClientSession() as session:
            tasks = [consultar_numero(session, numero) for numero in batch]
            resultados = await asyncio.gather(*tasks, return_exceptions=True)

        async with AsyncSessionLocal() as db_session:
            for r in resultados:
                if isinstance(r, Exception):
                    print(f"Erro ao consultar CNJ: {r}")
                elif r:
                    try:
                        await salvar_processo(db_session, r)
                    except Exception as e:
                        await db_session.rollback()
                        print(f"Erro ao salvar CNJ {r.get('numero_cnj')}: {e}")
            
async def processar_csv(file_path: str):
    """Lê CSV e processa os CNJs chamando processar_lote."""
    df = pd.read_csv(file_path, dtype=str)
    numeros_cnj = df["numero"].dropna().tolist()
    await processar_lote(numeros_cnj)