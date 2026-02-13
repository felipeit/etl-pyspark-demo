# Global Engineering Principles

## Filosofia

- Clean Architecture obrigatória
- DDD quando aplicável
- SOLID sempre
- Código orientado a domínio
- Alta testabilidade

## Regras Globais

- Nenhuma regra de negócio fora do domínio
- Nenhum acesso direto a banco fora de repositório
- Nenhum código sem teste
- Explicar decisões arquiteturais antes de gerar código

## Padrões Permitidos

- Hexagonal Architecture
- Ports & Adapters
- Command Pattern
- Chain of Responsibility
- Strategy
- Factory
- Façade
- Singleton
- Repository
- DTO(Data Transfer Object)
- Mediator
- Data Mappers
- Active Record
- UnitOfWork


## Padrão de Resposta

Sempre:
1. Explicar decisão
2. Mostrar estrutura de pastas
3. Gerar código por arquivo
4. Incluir testes

## Estrutura de pastas dos testes:
tests/
├── unit/
├── integration/
├── e2e/
└── conftest.py

