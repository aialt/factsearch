# FactSearch — Enhanced Fact-Checking Framework

## Overview

FactSearch is an AI pipeline for verifying the factuality of LLM outputs. Provide FactSearch with AI generated text, and FactSearch will provide a comprehensive factuality assessment.

### Quick Setup

Preresiquites:

- Python 3.10 or higher
- Docker Desktop
- Ollama (optional - if you want to run FactSearch locally)

1. Clone the repository:
```bash
git clone https://github.com/hmackenzie21/FactSearch
cd FactSearch
```

2. Run the setup script:
```bash
python setup.py
```

3. Activate the virtual environment:

**Mac/Linux:**
```bash
source .venv/bin/activate
```

**Windows:**
```bash
.venv\Scripts\activate
```

4. Start the application:
```bash
streamlit run app.py
```

5. Open your browser to `http://localhost:8501`


## Technical

The pipeline consists of a four-step verification process:

1. **Claim Extraction**: Given an LLM-generated response, the system extracts atomic factual claims. 
2. **Query Generation**: For each extracted claim, the system generates search queries intended to retrieve relevant supporting or refuting evidence.
3. **Evidence Retrieval**: Evidence is retrieved using a locally-hosted instance of **SearXNG**, an open-source meta-search engine.
4. **Claim Verification**: For each claim, reteieved evidence snippets are provided to a language model for evidence-conditioned reasoning. 

The pipeline outputs the following information:

* Binary Claim Verification Labels (True/False)
* Per-Claim Explanatory Reasoning for each decision 
* Source Links retrieved via SearXNG - some examples of commonly occuring sources are: Wikipedia, online newspaper articles, independent media publications, government/public-sector webpages (e.g. NHS pages for healthcare-related information), scholarly articles, and technical documentation. The nature of the sources used will vary depending on the information which the user inputs to the pipeline. 
* TXT/JSON exports of all the above (optional)

## AI Safety Disclaimer

FactSearch is a research project aimed at improving AI transparency and safety for decision-making scenarios, though it is important to acknowledge that it can make incorrect assumptions and fail to flag unfactual material. **Please do your due diligence in thoroughly checking LLM outputs, especially when this information is influencing important decisions.** FactSearch is intended only as a support tool and ultimately is not as a substitute for human intuition and expertise. 

## How to use FactSearch

Users can choose between classes of language model when initialising FactSearch - both have inherent advantages and disadvantages:

1. OpenAI Models: takes up no extra disk space as all NLP tasks are performed server side using OpenAI's black-box models. Incurs per-query costs and subject to OpenAI's non-transparent privacy policy.

2. Open Source Language Models: free to use and completely locally-hosted meaning user data is not passed to any third-parties. Requires disk space to store model weights. 

### FactSearch GUI

Upon load, users will be prompted to select an AI model which will be used for NLP tasks within the pipeline. 

The OpenAI models that FactSearch currently supports are:

1. GPT-5 
2. GPT-5-mini
3. GPT-5.2 

To run FactSearch with a GPT model, you will require an OpenAI API key, which you can get [here](https://developers.openai.com/api/docs/quickstart/).

You can find more info on OpenAI's models [here](https://developers.openai.com/api/docs/models).

Alternatively, users can choose to use the open-source model Ollama, in which case they will not require an API key and FactSearch will be completely free to use. 

Once FactSearch is initialised, users will be prompted for two inputs:

* Prompt/Question: Enter here the prompt that you passed to the LLM.
* Response to Check: Enter here the the LLM's response to your prompt. 

![FactSearch Input Panel](images/input_panel.png)

Once these fields have been filled, you will be able to trigger the fact checking pipeline and your results will be ready to review within a few moments. 


## Acknowledgements

Parts of the claim extraction and verification logic are adapted from [FacTool](https://github.com/GAIR-NLP/factool) (Chern et al., 2023), specifically the YAML prompt templates and OpenAI wrapper utilities (which have undergone modification to support current GPT models). All search infrastructure, evidence retrieval, UI components, and local model integration are original contributions.

This project was developed at the University of Liverpool by Harry Mackenzie and Dr Meng Fang. 



**Reference:**
> Chern, I., Chern, S., Chen, J., Yuan, W., Feng, K., Qin, C., ... & Liu, P. (2023). FacTool: Factuality Detection in Generative AI - A Tool Augmented Framework for Multi-Task and Multi-Domain Scenarios. *arXiv preprint arXiv:2307.13528*.
