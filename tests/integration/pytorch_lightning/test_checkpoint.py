"""Compatibility cases adapted from PyTorch Lightning.

Source repository: https://github.com/Lightning-AI/pytorch-lightning
Source release: lightning 2.6.5 (PyPI and Git tag 2.6.5)
Source cases:
- tests/tests_pytorch/trainer/connectors/test_checkpoint_connector.py:137-157,
  test_ckpt_for_fsspec
- tests/tests_pytorch/models/test_restore.py:438-477,
  test_load_model_from_checkpoint
"""

import torch
from lightning.pytorch import LightningModule, Trainer
from lightning.pytorch.callbacks import ModelCheckpoint
from torch.utils.data import DataLoader, TensorDataset


class TinyModel(LightningModule):
    def __init__(self):
        super().__init__()
        self.layer = torch.nn.Linear(1, 1)

    def training_step(self, batch, batch_idx):
        inputs, targets = batch
        return torch.nn.functional.mse_loss(self.layer(inputs), targets)

    def configure_optimizers(self):
        return torch.optim.SGD(self.parameters(), lr=0.1)


def test_model_checkpoint_roundtrip_through_opendal_url(opendal_url):
    """Save and load a Lightning checkpoint through its fsspec URL entry point."""
    checkpoint_dir = f"{opendal_url}/checkpoints"
    checkpoint = ModelCheckpoint(
        dirpath=checkpoint_dir,
        filename="model",
    )
    trainer = Trainer(
        max_epochs=1,
        accelerator="cpu",
        devices=1,
        logger=False,
        callbacks=[checkpoint],
        enable_progress_bar=False,
        enable_model_summary=False,
    )
    training_data = TensorDataset(
        torch.tensor([[1.0]]),
        torch.tensor([[2.0]]),
    )

    model = TinyModel()
    trainer.fit(model, train_dataloaders=DataLoader(training_data))
    restored = TinyModel.load_from_checkpoint(checkpoint.best_model_path)

    assert checkpoint.best_model_path == f"{checkpoint_dir}/model.ckpt"
    for name, value in model.state_dict().items():
        torch.testing.assert_close(restored.state_dict()[name], value)
